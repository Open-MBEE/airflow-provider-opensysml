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

### The connection

An `opensysml` connection names a `sysml-grpc` service by `host` and `port`.
Its `extra` may carry `version` (the release the service must report) and
`require_capabilities` (capability names it must advertise). Without a host, or
without a connection of that id at all, the client starts a private service for
the task and stops it when the task ends.

## Development

```sh
pip install -e ".[dev]"
ruff check . && ruff format --check .
pytest -m "not integration"                 # no service needed
OPENSYSML_BINARY=/path/to/sysml-grpc pytest  # the operator tests against a real service
```

## License

Apache-2.0, as OpenSysML.
