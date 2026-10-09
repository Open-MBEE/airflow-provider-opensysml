"""One question put to a model, asked the same way by operator, sensor and watcher.

A :class:`SysMLQuestion` names what is asked — a constraint, requirement,
satisfaction or object check as the client's verify methods ask it, or a
``case`` run through :meth:`~opensysml.Model.run_analysis` with arguments bound
to its ``in`` parameters — and :meth:`SysMLQuestion.ask` answers it as an
:class:`Answer`: whether it holds, why not, or why it could not be decided, with
the client's result as plain data for XCom.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from airflow_provider_opensysml.results import analysis_result_to_dict, verdict_to_dict

if TYPE_CHECKING:
    from opensysml import Model

VERIFY_KINDS: tuple[str, ...] = ("constraint", "requirement", "satisfy", "object")
QUESTION_KINDS: tuple[str, ...] = (*VERIFY_KINDS, "case")


@dataclass
class Answer:
    """The answer: ``holds``, else ``failures`` saying why not or ``error`` why nothing was decided."""

    holds: bool
    report: dict[str, Any]
    failures: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def decided(self) -> bool:
        return self.error is None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SysMLQuestion:
    """What is asked of a model.

    :param kind: ``constraint``, ``requirement``, ``satisfy`` or ``object`` ask the client's
        verify methods; ``case`` runs an analysis or verification case
    :param element: FQN of the element asked about; ``satisfy`` may leave it unset to check
        every satisfaction in the model
    :param subject: FQN of the part to instantiate and evaluate against
    :param engine: The analysis engine to ask (``auto`` when unset)
    :param question: ``evaluate`` (default), ``holds`` or ``satisfiable``, for constraints and requirements
    :param arguments: Positional arguments for a case's ``in`` parameters
    :param named_arguments: A case's arguments by parameter name
    :param schedule: The scheduling policy a case's actions resolve choice points under
    """

    kind: str
    element: str | None = None
    subject: str | None = None
    engine: str | None = None
    question: str | None = None
    arguments: list[Any] | None = None
    named_arguments: dict[str, Any] | None = None
    schedule: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in QUESTION_KINDS:
            raise ValueError(f"kind must be one of {', '.join(QUESTION_KINDS)}, not {self.kind!r}")
        if self.kind != "satisfy" and not self.element:
            raise ValueError(f"kind {self.kind!r} needs the element to verify")
        if self.kind in ("satisfy", "object") and (self.subject or self.question):
            raise ValueError(f"kind {self.kind!r} takes neither subject nor question")
        takes_arguments = self.arguments is not None or self.named_arguments is not None
        if self.kind != "case" and (takes_arguments or self.schedule):
            raise ValueError(f"kind {self.kind!r} takes no arguments or schedule; only a case does")
        if self.kind == "case" and self.question:
            raise ValueError("kind 'case' takes no question")

    def __str__(self) -> str:
        return f"{self.kind} {self.element}" if self.element else "every satisfaction"

    def as_dict(self) -> dict[str, Any]:
        """The question as keyword arguments, for a trigger's serialized form."""
        return asdict(self)

    def ask(self, model: Model) -> Answer:
        if self.kind == "case":
            return self._run_case(model)
        return self._verify(model)

    def _verify(self, model: Model) -> Answer:
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
        where = verdict.element or self.element
        if verdict.error:
            return Answer(False, report, error=f"{self.kind} {where} could not be decided: {verdict.error}")
        if verdict.holds:
            return Answer(True, report)
        return Answer(False, report, [f"{self.kind} {where} does not hold ({verdict.condition or 'false'})"])

    def _run_case(self, model: Model) -> Answer:
        result = model.run_analysis(
            self.element,
            subject=self.subject,
            arguments=self.arguments,
            named_arguments=self.named_arguments,
            schedule=self.schedule,
            engine=self.engine,
        )
        report = analysis_result_to_dict(result)
        failures: list[str] = []
        for verdict in result.verdicts:
            if verdict.error:
                undecided = f"{verdict.kind} {verdict.element} could not be decided: {verdict.error}"
                return Answer(False, report, error=f"{self.element}: {undecided}")
            if not verdict.holds:
                failures.append(
                    f"{verdict.kind} {verdict.element} does not hold ({verdict.condition or 'false'})"
                )
        for verification in result.verifications:
            if verification.kind == "error":
                detail = f": {verification.detail}" if verification.detail else ""
                errored = f"verification {verification.case_id} errored{detail}"
                return Answer(False, report, error=f"{self.element}: {errored}")
            if verification.kind != "pass":
                where = f"{verification.case_id} (subcase)" if verification.subcase else verification.case_id
                detail = f": {verification.detail}" if verification.detail else ""
                failures.append(f"verification {where} verdict: {verification.kind}{detail}")
        for evaluation in result.evaluations:
            if evaluation.error:
                failed = f"evaluation of {evaluation.function_id} failed: {evaluation.error}"
                return Answer(False, report, error=f"{self.element}: {failed}")
        if failures:
            return Answer(False, report, [f"{self.element}: " + "; ".join(failures)])
        return Answer(True, report)
