# Changelog

All notable changes to this provider are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added

- `sysml_model_asset`: a SysML v2 model, one file or a directory of them, as an Airflow
  asset whose watcher (`SysMLModelChangedTrigger`) fires an event when the model's content
  digest changes and settles.
- `SysMLAnalysisOperator` and `SysMLVerifyOperator`: analysis and verification cases, and the
  constraint, requirement, satisfaction and object questions, as tasks returning the answer as
  XCom and failing on a false verdict.
- `SysMLRequirementSensor` and `SysMLRequirementHoldsTrigger`: hold a task until a requirement
  holds, re-asking only when the model changes; `sysml_requirement_asset` and
  `SysMLRequirementSatisfiedTrigger`: an asset updated each time a requirement comes to hold.
- `SysMLActionDag`: Airflow tasks and dependencies from the `graphs:1` export of a SysML
  action, with a pluggable task factory (`KubernetesPodFactory` by default) and the model's
  `satisfy ... by <step>` statements as requirement gates before the steps they name.
- `OpenSysMLHook` and the `opensysml` connection type, naming an externally managed
  `sysml-grpc` service or starting a private one for the task.
- Examples: `lander_verification` (a DAG over a model) and `terrain_ncam` (the TIG M2020 NCAM
  terrain pipeline as a DAG generated from a model).

### Changed

- Requires Airflow 3.2 or later, the first whose task SDK carries the exceptions the provider
  imports from it (`airflow.sdk.exceptions`); the `kubernetes` extra requires
  `apache-airflow-providers-cncf-kubernetes` 10.0.0 or later. Both floors, and
  `opensysml` 0.12.0, are tested in CI beside the current releases on Python 3.10 through
  3.14.
- The package ships `py.typed`, and its source distribution carries the examples and the
  changelog beside the tests that read them.
- Requires `opensysml` 0.12.0 or later: OpenSysML 0.12.0 made a `Project`/`WhereFeature`
  read of a feature a row lacks a `missing-feature` error, and the `terrain_ncam` report's
  `StepTable` query now says `tolerate = true` to keep filtering the pipeline's flows and
  joins out, which 0.11.0 does not accept.
- A verdict's XCom report carries the `notes` its answer rests on, the `witness`
  assignment of a violated or satisfiable question and the engine's `standing`, which
  OpenSysML 0.12.0 reports beside each verdict.
- The `opensysml` connection form's hidden fields, labels and placeholders are declared in
  the provider manifest (`ui-field-behaviour`), which Airflow 3.2 and later read; the hook's
  `get_ui_field_behaviour` stays for older Airflow. Airflow 3.2 stopped reading the hook's
  without `flask-appbuilder`, which left the form generic.
