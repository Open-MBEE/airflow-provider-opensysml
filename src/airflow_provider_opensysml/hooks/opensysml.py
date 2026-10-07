"""A hook over the ``opensysml`` client and the ``sysml-grpc`` service it speaks to."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.sdk import BaseHook
from airflow.sdk.exceptions import AirflowNotFoundException

if TYPE_CHECKING:
    from opensysml import Connection, Model


class OpenSysMLHook(BaseHook):
    """Connect to a ``sysml-grpc`` service, or start a private one.

    An ``opensysml`` connection names an externally managed service by ``host``
    and ``port``. With no host — or no connection of that id at all — the client
    starts a private ``sysml-grpc`` child for the task, resolving the binary as
    the ``opensysml`` package documents (``$OPENSYSML_BINARY``, its cache, a
    download of the release it pins, then ``$PATH``), and stops it when the
    connection closes.

    The connection's ``extra`` may carry ``version`` (the release the service
    must report) and ``require_capabilities`` (capability names it must
    advertise); both are passed to :func:`opensysml.connect`.
    """

    conn_name_attr = "opensysml_conn_id"
    default_conn_name = "opensysml_default"
    conn_type = "opensysml"
    hook_name = "OpenSysML"

    def __init__(self, opensysml_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.opensysml_conn_id = opensysml_conn_id

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["login", "password", "schema"],
            "relabeling": {"host": "sysml-grpc host", "port": "sysml-grpc port"},
            "placeholders": {
                "host": "leave empty to start a private service",
                "port": "50051",
                "extra": '{"version": "v0.9.2", "require_capabilities": ["verification"]}',
            },
        }

    def _connection_settings(self) -> tuple[str | None, int | None, dict[str, Any]]:
        try:
            conn = self.get_connection(self.opensysml_conn_id)
        except AirflowNotFoundException:
            self.log.info(
                "No connection %r; a private sysml-grpc service will be started", self.opensysml_conn_id
            )
            return None, None, {}
        return conn.host or None, conn.port, conn.extra_dejson

    def get_conn(self) -> Connection:
        """Open an :class:`opensysml.Connection`; the caller closes it."""
        import opensysml

        host, port, extra = self._connection_settings()
        options = {
            "version": extra.get("version"),
            "require_capabilities": extra.get("require_capabilities"),
        }
        if host:
            self.log.info("Connecting to sysml-grpc at %s:%s", host, port or "default")
            return opensysml.connect(host, port, auto_start=False, **options)
        return opensysml.connect(auto_start=True, **options)

    def load(self, connection: Connection, model_path: str, *, strict: bool = True) -> Model:
        """Load the model at ``model_path`` over ``connection``; ``strict`` refuses one with errors."""
        return connection.load(model_path, strict=strict)
