"""Operators that run a SysML model's cases and checks as Airflow tasks.

Each operator loads the model through :class:`OpenSysMLHook`, asks the question,
and returns the answer as plain data for XCom. A task follows the ``sysml``
command line's exit-status contract: the task succeeds when every verdict
holds, fails when the model answered false, and fails with the client's error
when the question could not be answered at all.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowException

from airflow_provider_opensysml.hooks.opensysml import OpenSysMLHook
from airflow_provider_opensysml.results import analysis_result_to_dict, verdict_to_dict

if TYPE_CHECKING:
    from opensysml import Model

VERIFY_KINDS: tuple[str, ...] = ("constraint", "requirement", "satisfy", "object")


class SysMLBaseOperator(BaseOperator):
    """Load a model and answer one question about it.

    :param model_path: The ``.sysml``, ``.kerml`` or API-JSON file to load (templated)
    :param opensysml_conn_id: The ``opensysml`` connection naming the service
    :param strict: Refuse a model the service reports errors for
    :param fail_on_verdict: Fail the task when the model answers false; otherwise
        only report the verdict
    """

    template_fields: Sequence[str] = ("model_path",)
    ui_color = "#1f3b8a"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        model_path: str,
        opensysml_conn_id: str = OpenSysMLHook.default_conn_name,
        strict: bool = True,
        fail_on_verdict: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_path = model_path
        self.opensysml_conn_id = opensysml_conn_id
        self.strict = strict
        self.fail_on_verdict = fail_on_verdict

    def execute(self, context: Any) -> dict[str, Any]:
        hook = OpenSysMLHook(self.opensysml_conn_id)
        connection = hook.get_conn()
        try:
            model = hook.load(connection, self.model_path, strict=self.strict)
            self.log.info("Loaded %s (%d document(s))", self.model_path, len(model.documents))
            return self.answer(model)
        finally:
            connection.close()

    def answer(self, model: Model) -> dict[str, Any]:
        raise NotImplementedError

    def _fail(self, message: str) -> None:
        if self.fail_on_verdict:
            raise AirflowException(message)
        self.log.warning(message)


class SysMLAnalysisOperator(SysMLBaseOperator):
    """Run an analysis or verification case — a trade study included.

    The result is the case's ``outputs``, the ``verdicts`` of its objective and
    assertions, the body ``verifications`` of a verification case, the
    ``evaluations`` a trade study made, and the engine's ``standing``. The task
    fails when an objective or assertion does not hold, when a verification case's
    body produced anything but ``pass``, or when an evaluation failed.

    :param case: FQN of the analysis or verification case definition or usage (templated)
    :param subject: FQN of the part to run the case on; a usage binding its own subject needs none
    :param arguments: Positional arguments for the case's ``in`` parameters
    :param named_arguments: Arguments by parameter name
    :param engine: The analysis engine to ask (``auto`` when unset)
    :param schedule: The scheduling policy the case's actions resolve choice points under
    """

    template_fields: Sequence[str] = ("model_path", "case", "subject", "arguments", "named_arguments")

    def __init__(
        self,
        *,
        case: str,
        subject: str | None = None,
        arguments: list[Any] | None = None,
        named_arguments: dict[str, Any] | None = None,
        engine: str | None = None,
        schedule: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.case = case
        self.subject = subject
        self.arguments = arguments
        self.named_arguments = named_arguments
        self.engine = engine
        self.schedule = schedule

    def answer(self, model: Model) -> dict[str, Any]:
        result = model.run_analysis(
            self.case,
            subject=self.subject,
            arguments=self.arguments,
            named_arguments=self.named_arguments,
            schedule=self.schedule,
            engine=self.engine,
        )
        report = analysis_result_to_dict(result)
        for name, value in report["outputs"].items():
            self.log.info("%s.%s = %r", self.case, name, value)
        failures = []
        for verdict in result.verdicts:
            if verdict.error:
                raise AirflowException(
                    f"{self.case}: {verdict.kind} {verdict.element} could not be decided: {verdict.error}"
                )
            if not verdict.holds:
                failures.append(
                    f"{verdict.kind} {verdict.element} does not hold ({verdict.condition or 'false'})"
                )
        for verification in result.verifications:
            if verification.kind != "pass":
                where = f"{verification.case_id} (subcase)" if verification.subcase else verification.case_id
                detail = f": {verification.detail}" if verification.detail else ""
                failures.append(f"verification {where} verdict: {verification.kind}{detail}")
        for evaluation in result.evaluations:
            if evaluation.error:
                raise AirflowException(
                    f"{self.case}: evaluation of {evaluation.function_id} failed: {evaluation.error}"
                )
        if failures:
            self._fail(f"{self.case}: " + "; ".join(failures))
        else:
            self.log.info(
                "%s: every verdict holds (%s, %s)",
                self.case,
                result.standing.engine,
                result.standing.strength,
            )
        return report


class SysMLVerifyOperator(SysMLBaseOperator):
    """Ask whether a constraint or requirement holds, satisfactions hold, or an object is valid.

    ``kind`` selects the question, as the client's methods do: ``constraint``
    (:meth:`~opensysml.Model.verify_constraint`), ``requirement``
    (:meth:`~opensysml.Model.verify_requirement`), ``satisfy``
    (:meth:`~opensysml.Model.verify_satisfaction`) or ``object``
    (:meth:`~opensysml.Model.validate_instance`). The result is the verdict as
    plain data; the task fails when it does not hold.

    :param kind: One of ``constraint``, ``requirement``, ``satisfy``, ``object``
    :param element: FQN of the element verified (templated); ``satisfy`` may leave it unset to
        check every satisfaction in the model
    :param subject: FQN of the part to instantiate and evaluate against
    :param engine: The analysis engine to ask (``auto`` when unset)
    :param question: ``evaluate`` (default), ``holds`` or ``satisfiable``, for constraints and requirements
    """

    template_fields: Sequence[str] = ("model_path", "element", "subject")
    kinds: ClassVar[tuple[str, ...]] = VERIFY_KINDS

    def __init__(
        self,
        *,
        kind: str,
        element: str | None = None,
        subject: str | None = None,
        engine: str | None = None,
        question: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if kind not in self.kinds:
            raise ValueError(f"kind must be one of {', '.join(self.kinds)}, not {kind!r}")
        if kind != "satisfy" and not element:
            raise ValueError(f"kind {kind!r} needs the element to verify")
        if kind in ("satisfy", "object") and (subject or question):
            raise ValueError(f"kind {kind!r} takes neither subject nor question")
        self.kind = kind
        self.element = element
        self.subject = subject
        self.engine = engine
        self.question = question

    def answer(self, model: Model) -> dict[str, Any]:
        if self.kind == "constraint":
            verdict = model.verify_constraint(
                self.element, subject=self.subject, engine=self.engine, question=self.question
            )
        elif self.kind == "requirement":
            verdict = model.verify_requirement(
                self.element, subject=self.subject, engine=self.engine, question=self.question
            )
        elif self.kind == "satisfy":
            verdict = model.verify_satisfaction(self.element, engine=self.engine)
        else:
            verdict = model.validate_instance(self.element, engine=self.engine)
        report = verdict_to_dict(verdict)
        if verdict.error:
            raise AirflowException(
                f"{self.kind} {verdict.element or self.element} could not be decided: {verdict.error}"
            )
        if verdict.holds:
            self.log.info(
                "%s %s holds (%s)", self.kind, verdict.element or self.element, verdict.status or "holds"
            )
        else:
            where = verdict.element or self.element
            self._fail(f"{self.kind} {where} does not hold ({verdict.condition or 'false'})")
        return report
