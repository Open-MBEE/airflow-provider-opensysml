# airflow-provider-opensysml

An [Apache Airflow](https://airflow.apache.org) provider for
[OpenSysML](https://github.com/Open-MBEE/OpenSysML). A SysML v2 model is an
Airflow **asset**, and its analysis and verification cases are **tasks**:

- `sysml_model_asset(path)` builds an `Asset` whose watcher fires an asset event
  whenever the model's files change on disk, so a DAG scheduled on it runs on
  every save, checkout or copy.
- `SysMLAnalysisOperator` runs an analysis or verification case (trade studies
  included) and `SysMLVerifyOperator` asks whether a constraint, requirement,
  satisfaction or object holds. Both return the answer as plain data for XCom
  and fail the task when the model answers false.
- `SysMLRequirementSensor` waits until a requirement holds, deferring to a
  trigger that re-asks when the model changes; `sysml_requirement_asset` is an
  asset updated each time a requirement becomes satisfied, and `SysMLActionDag`
  turns the model's `satisfy ... by <step>` statements into gates before its steps.
- `OpenSysMLHook` reaches the `sysml-grpc` service through the `opensysml`
  client — an externally managed one named by an Airflow connection, or a
  private one started for the task.

Requires Airflow 3.0 or later (asset watchers are an Airflow 3 feature) and
Python 3.10 or later.

## Install

```sh
pip install airflow-provider-opensysml
```

The `opensysml` client resolves its `sysml-grpc` binary as its documentation
describes: `$OPENSYSML_BINARY`, then its cache, then a download of the release it
pins. Point `OPENSYSML_BINARY` at a build, or set up an `opensysml` connection
naming a running service, to avoid the download on workers.

## A DAG over a model

```python
from airflow.sdk import dag

from airflow_provider_opensysml.assets import sysml_model_asset
from airflow_provider_opensysml.operators import SysMLAnalysisOperator, SysMLVerifyOperator

MODEL = "/models/lander.sysml"
lander = sysml_model_asset(MODEL, name="lander_model", poll_interval=15)


@dag(schedule=[lander], catchup=False)
def lander_verification():
    budget = SysMLAnalysisOperator(task_id="fuel_budget", model_path=MODEL, case="Descent::scoutBudget")
    soft = SysMLVerifyOperator(
        task_id="soft_landing", model_path=MODEL, kind="requirement", element="Descent::scoutLandsSoftly"
    )
    check = SysMLAnalysisOperator(task_id="touchdown_check", model_path=MODEL, case="Descent::checkScout")
    [budget, soft] >> check


lander_verification()
```

`example_dags/lander_verification.py` is this DAG over the lander model from
OpenSysML's `analysis-demo`, with a summary task reading the asset event that
triggered the run.

### The asset

`sysml_model_asset` accepts a file or a directory. A directory is digested as
every `*.sysml` and `*.kerml` file under it (`patterns=` changes the selection),
so a multi-file model is one asset. The digest is over file contents, not
modification times: a checkout that rewrites identical files is not a change.

The watcher runs in the Airflow triggerer and polls every `poll_interval`
seconds; a change is reported once the digest has held still for
`settle_interval` seconds, so a save touching several files is one event. The
event's `extra` carries the new and previous digest, the file count and when the
change was observed, under `extra["payload"]` as Airflow records trigger-fired
events. The baseline is taken when the triggerer starts the
watcher; nothing fires for the state the model is already in. The path is
resolved where the DAG is parsed and must be visible to the triggerer under the
same path.

### The operators

Both operators take `model_path` (templated), `opensysml_conn_id`, `strict`
(refuse a model the service reports errors for; the default) and
`fail_on_verdict` (fail the task when the model answers false; the default — set
it to `False` to only report the verdict).

`SysMLAnalysisOperator(case, subject=, arguments=, named_arguments=, engine=,
schedule=)` runs the case as `sysml -analysis` does and returns its `outputs`,
`verdicts`, `verifications`, `evaluations` and the engine's `standing`. The task
fails when an objective or assertion does not hold, a verification case's body
produced anything but `pass`, or an evaluation failed; a question the engine
could not decide raises the client's error.

`SysMLVerifyOperator(kind, element=, subject=, engine=, question=)` asks one of
the client's questions — `constraint`, `requirement`, `satisfy` or `object` —
and returns the verdict.

### Waiting on a requirement

A task can be held back until a requirement of the model holds, and released the
moment it does. `SysMLRequirementSensor` asks the same question `SysMLVerifyOperator`
does — a `requirement`, `constraint`, `satisfy` or `object` check, or a `case` run
with arguments bound to its `in` parameters — and succeeds once the answer holds, with
the answer as its XCom value:

```python
from airflow_provider_opensysml.sensors import SysMLRequirementSensor

reserve_held = SysMLRequirementSensor(
    task_id="reserve_held",
    model_path=MODEL,
    element="Descent::scoutLandsSoftly",   # kind="requirement" by default
    deferrable=True,                       # wait in the triggerer, not a worker slot
    poke_interval=60,
    timeout=6 * 3600,
)
reserve_held >> land
```

Deferred, the sensor hands the wait to `SysMLRequirementHoldsTrigger`, which re-asks
only when the model's files change (the same digest watch as the asset) rather than
on a clock. An answer the model cannot decide — an unknown element, an unbound
parameter — fails the sensor; set `fail_on_undecided=False` to keep waiting instead.

A requirement can only come to hold when something it is evaluated over changes: the
model's files, or the values bound to a case's `in` parameters. The latter is how a
run's own facts reach the model without being written into it — `named_arguments` is
templated, so run configuration flows in:

```python
pair_matches = SysMLRequirementSensor(
    task_id="pair_matches",
    model_path=MODEL,
    kind="case",
    element="TerrainNCAM::Verification::stereoPairCheck",
    named_arguments={
        "leftAcquisition": "{{ acquisition(dag_run.conf['left_key']) }}",
        "rightAcquisition": "{{ acquisition(dag_run.conf['right_key']) }}",
        "leftEye": "{{ eye(dag_run.conf['left_key']) }}",
        "rightEye": "{{ eye(dag_run.conf['right_key']) }}",
    },
)
```

`VerifyRequirement` itself takes no bindings, so a requirement that depends on a run's
values is stated with `in` parameters and run through a verification case (see
`StereoPairCheck` in `example_dags/models/terrain_ncam.sysml`). A fact outside the model
altogether — a product landing in an object store — is still a job for an ordinary
Airflow sensor, or for a case it is bound into.

To start a whole DAG as soon as a requirement is met, schedule it on
`sysml_requirement_asset`: its watcher, `SysMLRequirementSatisfiedTrigger`, asks the
question at each change of the model's files and emits one asset event each time the
answer turns from not holding to holding — never for a model that goes on holding it:

```python
from airflow_provider_opensysml.assets import sysml_requirement_asset

with DAG(dag_id="land", schedule=[sysml_requirement_asset(MODEL, "Descent::scoutLandsSoftly")]):
    ...
```

### The connection

An `opensysml` connection names a `sysml-grpc` service by `host` and `port`.
Its `extra` may carry `version` (the release the service must report) and
`require_capabilities` (capability names it must advertise). Without a host, or
without a connection of that id at all, the client starts a private service for
the task and stops it when the task ends.

## A DAG generated from a model

A SysML v2 `action def` can also *be* the DAG. OpenSysML lowers a behavior into
a canonical `graphs:1` JSON graph (nodes, successions, flows, parameters and
attributes), and `SysMLActionDag` turns the graph of an action into Airflow
tasks and `>>` dependencies:

- one task per action node of the subject's graph, named after the node; a
  node that performs another behavior is one task described by that behavior's
  attributes and parameters, and the behavior's own graph is not expanded;
- a succession is an edge, followed through forks, joins, merges and decisions
  so a fork fans out and a join fans in; a `flow` from a pin of one step into a
  pin of another also puts the producer upstream; edges a longer path already
  implies are dropped;
- a pluggable task factory builds each operator. The default,
  `KubernetesPodFactory`, runs `<wrapper_dir>/<wrapper>` in the step's `image`
  with requests and limits from its `cpu` and `memoryGi` attributes, taking
  the step's attributes from the performed behavior and falling back to the
  subject action's own (a pipeline-wide `image`). Anything else, down to an
  `EmptyOperator`, is one function `(ActionStep, DAG) -> BaseOperator`.

`example_dags/models/terrain_ncam.sysml` models the
[TIG](https://github.com/NASA-AMMOS/tig) M2020 NCAM terrain pipeline this way:
`part def`s for the FDR, RAS, DSP, XYM and mesh products with the M20 filename
fields, `action def`s for the four VICAR steps with typed `in`/`out` pins and
the wrapper, cpu, memory and OpenMP thread count each pod needs, an
`action def Terrain` composing them with typed flows, successions and fork/join
over the two eyes, requirements with verification cases, and a DocGen report.
`example_dags/terrain_ncam.py` builds the DAG from its export and reproduces the
eight tasks and eight edges of TIG's hand-written `ids_terrain_ncam.py`; the
keys, bucket and run id of a run are not in the model but in `dag_run.conf`.

```sh
sysml example_dags/models/terrain_ncam.sysml                                   # loads clean
sysml -satisfy example_dags/models/terrain_ncam.sysml                          # the requirements hold
sysml example_dags/models/terrain_ncam.sysml -graphs TerrainNCAM::Pipeline::Terrain \
      -o example_dags/models/terrain_ncam.graphs.json                          # the graph the DAG is built from
sysml example_dags/models/terrain_ncam.sysml \
      -render-document TerrainNCAM::Report::TerrainReport -doc-form html -o terrain.html
```

```python
from airflow_provider_opensysml.dag import KubernetesPodFactory, SysMLActionDag

# From a graph the CLI exported; works with any opensysml release.
terrain = SysMLActionDag.from_file("models/terrain_ncam.graphs.json", task_factory=KubernetesPodFactory())
# Or exported live through the hook at parse time (needs the develop client, see below).
terrain = SysMLActionDag.from_model("models/terrain_ncam.sysml", "TerrainNCAM::Pipeline::Terrain")

with DAG("terrain", schedule=None, ...) as dag:
    terrain.build(dag)
```

Trigger the example with the run's inputs:

```sh
airflow dags trigger opensysml_terrain_ncam --conf '{"bucket": "ids-pipeline",
  "left_key": "input/NLM_1835_0829848458_777FDR_N0874924NCAM00230_0A02LLJ01.VIC",
  "right_key": "input/NRM_1835_0829848458_777FDR_N0874924NCAM00230_0A02LLJ01.VIC"}'
```

The pods need the `apache-airflow-providers-cncf-kubernetes` provider
(`pip install "airflow-provider-opensysml[kubernetes]"`), a cluster with TIG's
`tig-worker` image, `vicar-wrappers` config map and calibration volume, and the
S3 credentials in the `tig_s3_access_key`/`tig_s3_secret_key` variables.

### Gates from the model

The model also says which step answers for each requirement, with ordinary
`satisfy` statements:

```sysml
satisfy correlateLeftCpu by ncamWorker.terrain.correlate_left;
satisfy eyesFitTogether by ncamWorker;
```

`SysMLActionDag` turns each into a gate: a `SysMLRequirementSensor` task
(`require_TN-3`) upstream of the step it names, or of every first step when the
satisfying element is the worker or the pipeline itself, released once the requirement
holds. `from_model(..., gates=True)` reads them from the model; from a file, pass
`gates=load_gates("models/terrain_ncam.gates.json")` and a `RequirementGateFactory`:

```python
gates = RequirementGateFactory(MODEL, deferrable=True, poke_interval=30, timeout=3600)
terrain = SysMLActionDag.from_file(GRAPHS, task_factory=pods, gates=load_gates(GATES), gate_factory=gates)
```

The example DAG does this for TN-1..TN-10 and, for TN-12 (this run's stereo pair),
adds a `case` sensor bound from `dag_run.conf` before either eye is correlated. The
same model renders the pipeline description —
`sysml example_dags/models/terrain_ncam.sysml -render-document TerrainNCAM::Report::TerrainReport -doc-form html -o terrain.html`
— with the step and product tables, the flow diagram, the requirement text and the
traceability matrix.

### Until the next OpenSysML release

Graph export (`sysml -graphs`, `Model.export_graphs`) is on OpenSysML's
`develop` branch and not yet in a released `opensysml` wheel. Until it is, build
the service and install the client from a checkout (Go 1.25 or later):

```sh
git clone --branch develop https://github.com/Open-MBEE/OpenSysML.git
cd OpenSysML && make build          # bin/sysml, bin/sysml-grpc, ...
pip install -e client/python
export OPENSYSML_BINARY=$PWD/bin/sysml-grpc   # the client uses this build instead of a release
```

CI does the same. Users on a released client are not blocked: export the graph
with the CLI and build the DAG from the file with `SysMLActionDag.from_file`.

## Development

```sh
pip install -e ".[dev]"
ruff check . && ruff format --check .
pytest -m "not integration"                 # no service needed
OPENSYSML_BINARY=/path/to/sysml-grpc pytest  # the operator and live-export tests against a real service
```

## License

Apache-2.0, as OpenSysML.
