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

- Requires `opensysml` 0.11.0 or later, the first release carrying graph export and the
  verification questions, so no OpenSysML source build is needed.
