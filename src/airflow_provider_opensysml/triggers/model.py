"""An event trigger that fires when a SysML model changes on disk."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timezone
from typing import Any

from airflow.triggers.base import BaseEventTrigger, TriggerEvent

from airflow_provider_opensysml.digest import DEFAULT_PATTERNS, ModelDigest, digest_model


class SysMLModelChangedTrigger(BaseEventTrigger):
    """Fire an event each time the digest of a SysML model's files changes.

    The trigger digests ``path`` — one file, or every ``*.sysml``/``*.kerml``
    file under a directory — every ``poll_interval`` seconds and yields a
    :class:`TriggerEvent` when the digest differs from the last one it reported.
    A change is reported only once the digest has held still for
    ``settle_interval`` seconds, so a save that rewrites several files is one
    event rather than one per file. The baseline is taken when the trigger
    starts: nothing fires for the state the model is already in.

    The event payload names the ``path``, the new ``digest``, the
    ``previous_digest``, the number of ``files`` read and when the change was
    ``observed_at``; Airflow records it as the asset event's ``extra``.

    :param path: The model file or directory to watch
    :param patterns: Glob patterns selecting files under a directory
    :param poll_interval: Seconds between digests
    :param settle_interval: Seconds a changed digest must hold before it is reported
    """

    def __init__(
        self,
        path: str,
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
        self.path = str(path)
        self.patterns = list(patterns) if patterns else list(DEFAULT_PATTERNS)
        self.poll_interval = float(poll_interval)
        self.settle_interval = float(settle_interval)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_opensysml.triggers.model.SysMLModelChangedTrigger",
            {
                "path": self.path,
                "patterns": self.patterns,
                "poll_interval": self.poll_interval,
                "settle_interval": self.settle_interval,
            },
        )

    async def _digest(self) -> ModelDigest:
        return await asyncio.to_thread(digest_model, self.path, self.patterns)

    async def _settled(self, changed: ModelDigest) -> ModelDigest:
        """Re-digest until the model holds still, and return what it settled on."""
        while True:
            await asyncio.sleep(self.settle_interval)
            again = await self._digest()
            if again == changed:
                return again
            changed = again

    async def run(self) -> AsyncIterator[TriggerEvent]:
        last = await self._digest()
        self.log.info("Watching %s (%d file(s), %s)", self.path, last.files, last.digest)
        while True:
            await asyncio.sleep(self.poll_interval)
            current = await self._digest()
            if current.digest == last.digest:
                continue
            current = await self._settled(current)
            if current.digest == last.digest:
                continue
            self.log.info(
                "%s changed: %s -> %s (%d file(s))", self.path, last.digest, current.digest, current.files
            )
            yield TriggerEvent(
                {
                    "path": self.path,
                    "digest": current.digest,
                    "previous_digest": last.digest,
                    "files": current.files,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            last = current
