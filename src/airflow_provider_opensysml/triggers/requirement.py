"""Triggers that watch whether a question put to a SysML model holds.

Both re-ask the question only when the model's files change — the answer to an
unchanged model with unchanged arguments is the answer already given — through
the same digest watch :class:`~airflow_provider_opensysml.triggers.model.SysMLModelChangedTrigger`
uses. :class:`SysMLRequirementHoldsTrigger` resumes a deferred
:class:`~airflow_provider_opensysml.sensors.sysml.SysMLRequirementSensor` once the
answer holds; :class:`SysMLRequirementSatisfiedTrigger` is an asset watcher that
fires an event each time the answer turns from not holding to holding.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timezone
from typing import Any

from airflow.triggers.base import BaseEventTrigger, BaseTrigger, TriggerEvent

from airflow_provider_opensysml.digest import DEFAULT_PATTERNS
from airflow_provider_opensysml.hooks.opensysml import OpenSysMLHook
from airflow_provider_opensysml.questions import Answer, SysMLQuestion
from airflow_provider_opensysml.triggers.model import ModelWatch


def ask_model(model_path: str, question: SysMLQuestion, opensysml_conn_id: str, strict: bool) -> Answer:
    """Load the model through the hook and ask ``question`` of it."""
    hook = OpenSysMLHook(opensysml_conn_id)
    connection = hook.get_conn()
    try:
        return question.ask(hook.load(connection, model_path, strict=strict))
    finally:
        connection.close()


class _QuestionWatch:
    """What the two triggers share: the question, the hook, and the model watch."""

    def __init__(
        self,
        model_path: str,
        question: dict[str, Any],
        opensysml_conn_id: str = OpenSysMLHook.default_conn_name,
        strict: bool = True,
        patterns: Sequence[str] | None = None,
        poll_interval: float = 30.0,
        settle_interval: float = 2.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if poll_interval <= 0:
            raise ValueError(f"poll_interval must be positive, not {poll_interval!r}")
        if settle_interval < 0:
            raise ValueError(f"settle_interval must not be negative, not {settle_interval!r}")
        self.model_path = str(model_path)
        self.question = SysMLQuestion(**question)
        self.opensysml_conn_id = opensysml_conn_id
        self.strict = strict
        self.patterns = list(DEFAULT_PATTERNS) if patterns is None else list(patterns)
        self.poll_interval = float(poll_interval)
        self.settle_interval = float(settle_interval)

    def _kwargs(self) -> dict[str, Any]:
        return {
            "model_path": self.model_path,
            "question": self.question.as_dict(),
            "opensysml_conn_id": self.opensysml_conn_id,
            "strict": self.strict,
            "patterns": self.patterns,
            "poll_interval": self.poll_interval,
            "settle_interval": self.settle_interval,
        }

    def _watch(self) -> ModelWatch:
        return ModelWatch(self.model_path, self.patterns, self.poll_interval, self.settle_interval)

    async def _ask(self) -> Answer:
        return await asyncio.to_thread(
            ask_model, self.model_path, self.question, self.opensysml_conn_id, self.strict
        )


class SysMLRequirementHoldsTrigger(_QuestionWatch, BaseTrigger):
    """Resume once the question holds of the model, re-asking it at each change of the model's files.

    Fires one event: ``{"holds": True, "report": ...}`` when the answer holds, or
    ``{"holds": False, "error": ...}`` when it could not be decided. An answer that
    does not hold keeps waiting for the model to change.

    :param model_path: The model file or directory, loaded through the hook and watched for changes
    :param question: The :class:`SysMLQuestion` as keyword arguments
    :param opensysml_conn_id: The ``opensysml`` connection naming the service
    :param strict: Refuse a model the service reports errors for
    :param patterns: Glob patterns selecting files under a directory
    :param poll_interval: Seconds between digests of the model
    :param settle_interval: Seconds a change must hold still before the model is re-asked
    """

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_opensysml.triggers.requirement.SysMLRequirementHoldsTrigger",
            self._kwargs(),
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        watch = self._watch()
        digest = await watch.start()
        answer = await self._ask()
        changes = watch.changes()
        while answer.decided and not answer.holds:
            self.log.info(
                "%s does not hold of %s (%s); waiting for the model to change",
                self.question,
                self.model_path,
                digest.digest,
            )
            _, digest = await changes.__anext__()
            answer = await self._ask()
        if answer.holds:
            self.log.info("%s holds of %s (%s)", self.question, self.model_path, digest.digest)
            yield TriggerEvent({"holds": True, "report": answer.report, "digest": digest.digest})
        else:
            yield TriggerEvent({"holds": False, "error": answer.error, "digest": digest.digest})


class SysMLRequirementSatisfiedTrigger(_QuestionWatch, BaseEventTrigger):
    """Fire an event each time the question's answer turns from not holding to holding.

    The question is asked when the watcher starts and again at each settled change
    of the model's files. An answer that holds when the one before it did not — or
    could not be decided — is an event; one that goes on holding is not, so a DAG
    scheduled on the asset runs once per time the requirement becomes satisfied.
    Nothing fires for a model that already satisfies it when the watcher starts.

    The payload names the ``path``, the ``digest`` it was decided on, the
    ``question`` asked, the ``report`` of the answer and when it was ``observed_at``.
    """

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_opensysml.triggers.requirement.SysMLRequirementSatisfiedTrigger",
            self._kwargs(),
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        watch = self._watch()
        digest = await watch.start()
        previous = await self._ask()
        self.log.info(
            "Watching %s for %s to hold (now: %s, %s)",
            self.model_path,
            self.question,
            self._state(previous),
            digest.digest,
        )
        async for _, digest in watch.changes():
            answer = await self._ask()
            self.log.info(
                "%s changed (%s): %s %s", self.model_path, digest.digest, self.question, self._state(answer)
            )
            if answer.holds and not previous.holds:
                yield TriggerEvent(
                    {
                        "path": self.model_path,
                        "digest": digest.digest,
                        "question": str(self.question),
                        "report": answer.report,
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
            previous = answer

    @staticmethod
    def _state(answer: Answer) -> str:
        if answer.holds:
            return "holds"
        return "undecided: " + (answer.error or "") if not answer.decided else "does not hold"
