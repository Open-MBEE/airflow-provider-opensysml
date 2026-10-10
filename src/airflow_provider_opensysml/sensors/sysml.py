"""A sensor that waits until a question put to a SysML model holds."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from airflow.configuration import conf
from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue
from airflow.sdk.exceptions import AirflowException

from airflow_provider_opensysml.hooks.opensysml import OpenSysMLHook
from airflow_provider_opensysml.questions import QUESTION_KINDS, SysMLQuestion
from airflow_provider_opensysml.triggers.requirement import SysMLRequirementHoldsTrigger, ask_model


class SysMLRequirementSensor(BaseSensorOperator):
    """Wait until a requirement — or any question :class:`SysMLQuestion` can ask — holds of the model.

    The sensor asks at each poke and succeeds, with the answer's report as its
    XCom value, once the answer holds. Deferred (``deferrable=True``, or the
    ``operators.default_deferrable`` setting), it hands the wait to
    :class:`~airflow_provider_opensysml.triggers.requirement.SysMLRequirementHoldsTrigger`,
    which re-asks only when the model's files change rather than on a clock. A
    question the model cannot decide fails the sensor, unless ``fail_on_undecided``
    is off, in which case it is treated as not holding yet.

    A downstream task placed after the sensor therefore runs only once the
    requirement is met, and as soon as it is.

    :param model_path: The model file or directory to load (templated)
    :param kind: ``constraint``, ``requirement``, ``satisfy``, ``object``, or ``case`` for an
        analysis or verification case run with arguments
    :param element: FQN of the element asked about (templated)
    :param subject: FQN of the part to instantiate and evaluate against (templated)
    :param engine: The analysis engine to ask (``auto`` when unset)
    :param question: ``evaluate`` (default), ``holds`` or ``satisfiable``
    :param arguments: Positional arguments for a case's ``in`` parameters (templated)
    :param named_arguments: A case's arguments by parameter name (templated)
    :param schedule: The scheduling policy a case's actions resolve choice points under
    :param opensysml_conn_id: The ``opensysml`` connection naming the service
    :param strict: Refuse a model the service reports errors for
    :param fail_on_undecided: Fail when the model cannot decide the question
    :param deferrable: Wait in the triggerer rather than a worker slot
    :param patterns: Glob patterns selecting the files watched under a directory, when deferred
    :param settle_interval: Seconds a change must hold still before the model is re-asked, when deferred
    """

    template_fields: Sequence[str] = ("model_path", "element", "subject", "arguments", "named_arguments")
    ui_color = "#1f3b8a"
    ui_fgcolor = "#ffffff"
    kinds = QUESTION_KINDS

    def __init__(
        self,
        *,
        model_path: str,
        kind: str = "requirement",
        element: str | None = None,
        subject: str | None = None,
        engine: str | None = None,
        question: str | None = None,
        arguments: list[Any] | None = None,
        named_arguments: dict[str, Any] | None = None,
        schedule: str | None = None,
        opensysml_conn_id: str = OpenSysMLHook.default_conn_name,
        strict: bool = True,
        fail_on_undecided: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        patterns: Sequence[str] | None = None,
        settle_interval: float = 2.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        SysMLQuestion(kind, element, subject=subject, question=question, schedule=schedule)
        self.model_path = model_path
        self.kind = kind
        self.element = element
        self.subject = subject
        self.engine = engine
        self.question = question
        self.arguments = arguments
        self.named_arguments = named_arguments
        self.schedule = schedule
        self.opensysml_conn_id = opensysml_conn_id
        self.strict = strict
        self.fail_on_undecided = fail_on_undecided
        self.deferrable = deferrable
        self.patterns = None if patterns is None else list(patterns)
        self.settle_interval = settle_interval

    def sysml_question(self) -> SysMLQuestion:
        """The question, with its templated fields rendered."""
        return SysMLQuestion(
            self.kind,
            self.element,
            subject=self.subject,
            engine=self.engine,
            question=self.question,
            arguments=self.arguments,
            named_arguments=self.named_arguments,
            schedule=self.schedule,
        )

    def poke(self, context: Any) -> PokeReturnValue | bool:
        question = self.sysml_question()
        answer = ask_model(self.model_path, question, self.opensysml_conn_id, self.strict)
        if answer.error:
            if self.fail_on_undecided:
                raise AirflowException(answer.error)
            self.log.warning("%s; treating as not yet holding", answer.error)
            return False
        if answer.holds:
            self.log.info("%s holds of %s", question, self.model_path)
            return PokeReturnValue(is_done=True, xcom_value=answer.report)
        self.log.info("%s does not hold yet: %s", question, "; ".join(answer.failures))
        return False

    def execute(self, context: Any) -> Any:
        if not self.deferrable:
            return super().execute(context)
        poked = self.poke(context)
        if isinstance(poked, PokeReturnValue) and poked.is_done:
            return poked.xcom_value
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=SysMLRequirementHoldsTrigger(
                model_path=self.model_path,
                question=self.sysml_question().as_dict(),
                opensysml_conn_id=self.opensysml_conn_id,
                strict=self.strict,
                patterns=self.patterns,
                poll_interval=self.poke_interval,
                settle_interval=self.settle_interval,
                fail_on_undecided=self.fail_on_undecided,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: dict[str, Any] | None = None) -> Any:
        question = self.sysml_question()
        if not event or not event.get("holds"):
            error = (event or {}).get("error") or "the trigger ended without an answer"
            raise AirflowException(f"{question}: {error}")
        self.log.info("%s holds of %s (%s)", question, self.model_path, event.get("digest"))
        return event["report"]
