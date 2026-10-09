"""The M2020 NCAM terrain pipeline, generated from its SysML v2 model.

``models/terrain_ncam.sysml`` models the pipeline as ``action def Terrain``; its
``graphs:1`` export (``models/terrain_ncam.graphs.json``, written by
``sysml models/terrain_ncam.sysml -graphs TerrainNCAM::Pipeline::Terrain -o ...``)
is what this DAG is built from, so the task ids, the fan-out over both eyes and
every dependency come from the model. Set ``OPENSYSML_TERRAIN_LIVE_EXPORT=1`` to
export the graph from the model through the hook at parse time instead.

The model holds the pipeline type only. One run's inputs arrive in
``dag_run.conf`` as the TIG pipeline expects them::

    {"bucket": "ids-pipeline",
     "left_key":  "input/NLM_1835_0829848458_777FDR_N0874924NCAM00230_0A02LLJ01.VIC",
     "right_key": "input/NRM_1835_0829848458_777FDR_N0874924NCAM00230_0A02LLJ01.VIC"}

and the S3 keys of every product are derived from them with the M20 filename
convention, exactly as the hand-written TIG DAG does.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG
from kubernetes.client import models as k8s

from airflow_provider_opensysml.dag import (
    ActionStep,
    KubernetesPodFactory,
    RequirementGateFactory,
    SysMLActionDag,
    load_gates,
)
from airflow_provider_opensysml.sensors import SysMLRequirementSensor

MODELS = Path(__file__).parent / "models"
MODEL = os.environ.get("OPENSYSML_EXAMPLE_TERRAIN_MODEL") or str(MODELS / "terrain_ncam.sysml")
GRAPHS = os.environ.get("OPENSYSML_EXAMPLE_TERRAIN_GRAPHS") or str(MODELS / "terrain_ncam.graphs.json")
GATES = os.environ.get("OPENSYSML_EXAMPLE_TERRAIN_GATES") or str(MODELS / "terrain_ncam.gates.json")
STEREO_PAIR_CHECK = "TerrainNCAM::Verification::stereoPairCheck"
SUBJECT = "TerrainNCAM::Pipeline::Terrain"

S3_ENDPOINT = os.environ.get("TIG_S3_ENDPOINT", "http://minio.tig-airflow.svc.cluster.local:9000")
NAMESPACE = os.environ.get("TIG_NAMESPACE", "tig-airflow")
S3_ACCESS_KEY = "{{ var.value.get('tig_s3_access_key', 'minioadmin') }}"
S3_SECRET_KEY = "{{ var.value.get('tig_s3_secret_key', 'minioadmin') }}"


# --- M20 product naming, registered as Jinja macros ---------------------------


def _basename(key: str) -> str:
    return key.rsplit("/", 1)[-1].rsplit(".", 1)[0]


def fdr_to_ras_base(fdr_key: str) -> str:
    """FDR key -> RAS basename: char 19 '_' -> 'M' (venue), chars 23:26 'FDR' -> 'RAS'."""
    b = _basename(fdr_key)
    if b[19] != "_" or b[23:26] != "FDR":
        raise ValueError(f"unexpected FDR basename layout: {b!r}")
    return b[:19] + "M" + b[20:23] + "RAS" + b[26:]


def product_name(ras_base: str, product_type: str, ext: str) -> str:
    """Swap the product-type field [23:26] of a RAS basename and set the extension."""
    if len(product_type) != 3:
        raise ValueError(f"product_type must be 3 chars: {product_type!r}")
    return f"{ras_base[:23]}{product_type}{ras_base[26:]}.{ext}"


def acquisition(fdr_key: str) -> str:
    """The fields of an FDR basename both eyes of a pair share: all but instrument/eye and product type."""
    b = _basename(fdr_key)
    return b[4:23] + b[26:]


def eye(fdr_key: str) -> str:
    """The eye field of an FDR basename: ``L`` or ``R``."""
    return _basename(fdr_key)[1]


def sol_path(ras_base: str) -> str:
    return f"sol/{int(ras_base[4:8]):05d}"


def ods_prefix(ras_base: str) -> str:
    return f"output/{sol_path(ras_base)}/ids/rdr/ncam"


# --- Pins -> S3 keys ----------------------------------------------------------

BUCKET = "{{ dag_run.conf['bucket'] }}"
RUN_ID = "{{ run_id }}"
PQ = "processque/{{ run_id }}"


def _eye(task_id: str) -> str:
    return task_id.rsplit("_", 1)[-1]


def _other(eye: str) -> str:
    return "right" if eye == "left" else "left"


def fdr_key(eye: str) -> str:
    return f"{{{{ dag_run.conf['{eye}_key'] }}}}"


def ras_base(eye: str) -> str:
    return f"{{{{ fdr_to_ras_base(dag_run.conf['{eye}_key']) }}}}"


def ras_key(eye: str) -> str:
    return f"{PQ}/{ras_base(eye)}.VIC"


def derived_key(eye: str, product_type: str, ext: str) -> str:
    name = f"product_name(fdr_to_ras_base(dag_run.conf['{eye}_key']), '{product_type}', '{ext}')"
    return f"{PQ}/{{{{ {name} }}}}"


def ods(eye: str) -> str:
    return f"{{{{ ods_prefix(fdr_to_ras_base(dag_run.conf['{eye}_key'])) }}}}"


def pin_key(step: ActionStep, pin: str) -> str:
    """The S3 key of the product on one pin of a step, from the eye it belongs to."""
    eye = _eye(step.task_id)
    product = (step.inputs | step.outputs)[pin].rsplit("::", 1)[-1]
    flow = step.pin_source(pin)
    if flow is not None:
        eye = _eye(flow.producer)
    if product == "FDR":
        return fdr_key(eye)
    if product == "RAS":
        return ras_key(eye)
    if product == "DSP":
        return derived_key(eye, "DSP", "img")
    if product == "XYM":
        return derived_key(eye, "XYM", "xym")
    raise ValueError(f"no key rule for {product} on {step.task_id}.{pin}")


def wrapper_arguments(step: ActionStep) -> list[str]:
    """The arguments of each TIG wrapper after ``<s3_endpoint> <bucket>``."""
    eye = _eye(step.task_id)
    wrapper = step.attributes["wrapper"]
    if wrapper == "rad_wrapper.sh":
        return [pin_key(step, "fdr"), pin_key(step, "ras"), RUN_ID]
    if wrapper == "correlate_wrapper.sh":
        return [pin_key(step, "eye"), pin_key(step, "other"), pin_key(step, "disparity"), RUN_ID, eye]
    if wrapper == "xyz_wrapper.sh":
        pins = ("eye", "other", "disparity", "xym")
        return [*(pin_key(step, pin) for pin in pins), RUN_ID, eye]
    if wrapper == "mesh_wrapper.sh":
        return [pin_key(step, "xym"), pin_key(step, "ras"), ods(eye), ras_base(eye), RUN_ID, eye]
    raise ValueError(f"no argument rule for wrapper {wrapper!r}")


def tig_arguments(step: ActionStep) -> list[str]:
    return [S3_ENDPOINT, BUCKET, *wrapper_arguments(step)]


wrapper_volume = k8s.V1Volume(
    name="wrappers",
    config_map=k8s.V1ConfigMapVolumeSource(name="vicar-wrappers", default_mode=0o755),
)
calib_volume = k8s.V1Volume(
    name="mars-calib", host_path=k8s.V1HostPathVolumeSource(path="/mnt/calib", type="Directory")
)

pods = KubernetesPodFactory(
    arguments=tig_arguments,
    pod_kwargs={
        "namespace": NAMESPACE,
        "env_vars": [
            k8s.V1EnvVar(name="AWS_ACCESS_KEY_ID", value=S3_ACCESS_KEY),
            k8s.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value=S3_SECRET_KEY),
            k8s.V1EnvVar(name="AWS_DEFAULT_REGION", value="us-west-2"),
            k8s.V1EnvVar(name="AWS_REQUEST_CHECKSUM_CALCULATION", value="when_required"),
        ],
        "volumes": [wrapper_volume, calib_volume],
        "volume_mounts": [
            k8s.V1VolumeMount(name="wrappers", mount_path="/opt/wrappers", read_only=True),
            k8s.V1VolumeMount(name="mars-calib", mount_path="/usr/local/vicar/mars_calib", read_only=True),
        ],
        "image_pull_policy": "IfNotPresent",
        "get_logs": True,
        "on_finish_action": "delete_pod",
    },
)

# The model's ``satisfy <requirement> by <step>`` statements become gates: a
# SysMLRequirementSensor before each step, released once the requirement holds.
gates = RequirementGateFactory(MODEL, poke_interval=30, timeout=3600)
if os.environ.get("OPENSYSML_TERRAIN_LIVE_EXPORT"):
    terrain = SysMLActionDag.from_model(MODEL, SUBJECT, task_factory=pods, gates=True, gate_factory=gates)
else:
    terrain = SysMLActionDag.from_file(GRAPHS, task_factory=pods, gates=load_gates(GATES), gate_factory=gates)

with DAG(
    dag_id="opensysml_terrain_ncam",
    description="M20 NCAM per-eye terrain meshes (FDR -> RAS -> DSP -> XYM -> mesh), from the SysML model",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={"bucket": "ids-pipeline", "left_key": "", "right_key": ""},
    user_defined_macros={
        "fdr_to_ras_base": fdr_to_ras_base,
        "product_name": product_name,
        "sol_path": sol_path,
        "ods_prefix": ods_prefix,
        "acquisition": acquisition,
        "eye": eye,
    },
    tags=["opensysml", "m2020", "ids", "terrain", "vicar"],
) as dag:
    tasks = terrain.build(dag)

    # TN-12 is about this run's pair, so its verification case is run with the
    # run's values bound to its in parameters, before either eye is correlated.
    stereo_pair = SysMLRequirementSensor(
        task_id="require_TN-12",
        model_path=MODEL,
        kind="case",
        element=STEREO_PAIR_CHECK,
        named_arguments={
            "leftAcquisition": "{{ acquisition(dag_run.conf['left_key']) }}",
            "rightAcquisition": "{{ acquisition(dag_run.conf['right_key']) }}",
            "leftEye": "{{ eye(dag_run.conf['left_key']) }}",
            "rightEye": "{{ eye(dag_run.conf['right_key']) }}",
        },
        poke_interval=30,
        timeout=600,
    )
    stereo_pair >> [tasks["correlate_left"], tasks["correlate_right"]]
