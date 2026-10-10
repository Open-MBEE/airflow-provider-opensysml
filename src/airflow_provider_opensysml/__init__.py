"""Apache Airflow provider for OpenSysML.

A SysML v2 model is an Airflow asset: :func:`sysml_model_asset` builds one whose
watcher fires whenever the model's files change on disk. Its analysis and
verification cases are tasks: :class:`SysMLAnalysisOperator` and
:class:`SysMLVerifyOperator` run them through the ``opensysml`` client and report
the result as XCom, failing the task when the model answers false. Its
requirements gate work: :class:`SysMLRequirementSensor` waits until one holds, and
:func:`sysml_requirement_asset` is an asset updated each time one becomes satisfied.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__", "get_provider_info"]


def get_provider_info() -> dict:
    """The provider manifest Airflow reads through the ``apache_airflow_provider`` entry point."""
    return {
        "package-name": "airflow-provider-opensysml",
        "name": "OpenSysML",
        "description": "SysML v2 models as Airflow assets; analysis and verification cases as tasks.",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "OpenSysML",
                "external-doc-url": "https://github.com/Open-MBEE/OpenSysML",
                "tags": ["software"],
            }
        ],
        "connection-types": [
            {
                "connection-type": "opensysml",
                "hook-class-name": "airflow_provider_opensysml.hooks.opensysml.OpenSysMLHook",
            }
        ],
        "hooks": [
            {
                "integration-name": "OpenSysML",
                "python-modules": ["airflow_provider_opensysml.hooks.opensysml"],
            }
        ],
        "operators": [
            {
                "integration-name": "OpenSysML",
                "python-modules": ["airflow_provider_opensysml.operators.sysml"],
            }
        ],
        "sensors": [
            {
                "integration-name": "OpenSysML",
                "python-modules": ["airflow_provider_opensysml.sensors.sysml"],
            }
        ],
        "triggers": [
            {
                "integration-name": "OpenSysML",
                "python-modules": [
                    "airflow_provider_opensysml.triggers.model",
                    "airflow_provider_opensysml.triggers.requirement",
                ],
            }
        ],
    }
