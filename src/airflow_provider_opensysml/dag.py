"""Build an Airflow DAG from the ``graphs:1`` export of a SysML v2 action.

OpenSysML lowers a behavior into a canonical graph (``sysml model.sysml -graphs
Pkg::Action``, or :meth:`opensysml.Model.export_graphs`). :class:`SysMLActionDag`
reads that export and turns the subject action into tasks and dependencies:

* **Tasks.** Every action node of the subject's own graph (an ``action`` usage or
  a ``perform``) becomes one task whose ``task_id`` is the node's name. A node that
  performs another behavior is still one task: the performed behavior's
  attributes and parameters describe the task, and its own graph, which the
  export carries alongside, is *not* expanded into further tasks. Control nodes
  (start, end, fork, join, merge, decision) become no task.
* **Dependencies.** A succession between two action nodes is a ``>>`` edge. A
  succession through control nodes is followed to the action nodes on either
  side, so a fork fans out and a join fans in. A ``flow`` from a pin of one
  action into a pin of another makes the producer upstream of the consumer, since
  the consumer needs the product before it starts. Edges a longer path already
  implies are dropped (transitive reduction), so a product that reaches a step
  through an intermediate step adds no edge of its own. Guards on decisions are
  not evaluated; every branch is scheduled.
* **Attributes.** The factory sees the literal attribute values of the performed
  behavior (``cpu = 2``), with the subject action's own attributes as fallback
  for anything the behavior leaves unset (a pipeline-wide ``image``).

A :data:`TaskFactory` turns each :class:`ActionStep` into an operator. The
default, :class:`KubernetesPodFactory`, runs a wrapper script from the
behavior's attributes in a pod sized by ``cpu`` and ``memoryGi``;
:func:`empty_task_factory` is for tests.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.sdk import DAG

__all__ = [
    "GRAPHS_VERSION",
    "ActionStep",
    "Flow",
    "KubernetesPodFactory",
    "SysMLActionDag",
    "TaskFactory",
    "empty_task_factory",
    "load_graphs",
]

GRAPHS_VERSION = 1

TASK_NODE_KINDS = frozenset({"action", "action usage", "perform", "perform action"})
CONTROL_NODE_KINDS = frozenset({"start", "end", "final", "fork", "join", "merge", "decision", "terminate"})


@dataclass(frozen=True)
class Flow:
    """A product moving from a pin of one step into a pin of another."""

    producer: str
    source_pin: str
    consumer: str
    target_pin: str


@dataclass(frozen=True)
class ActionStep:
    """One action node of the subject's graph, as a task factory sees it."""

    task_id: str
    kind: str
    performs: tuple[str, ...]
    attributes: Mapping[str, Any]
    inputs: Mapping[str, str]
    outputs: Mapping[str, str]
    flows_in: tuple[Flow, ...] = ()
    flows_out: tuple[Flow, ...] = ()

    @property
    def behavior(self) -> str | None:
        """The qualified name of the behavior the step performs, if any."""
        return self.performs[0] if self.performs else None

    def pin_source(self, pin: str) -> Flow | None:
        """The flow feeding one of this step's input pins."""
        return next((flow for flow in self.flows_in if flow.target_pin == pin), None)


TaskFactory = Callable[[ActionStep, "DAG"], "BaseOperator"]


def load_graphs(source: str | Path | Mapping[str, Any]) -> dict[str, Any]:
    """Parse a ``graphs`` export from JSON text, a file path or a mapping, and check its version."""
    if isinstance(source, Mapping):
        graphs: dict[str, Any] = dict(source)
    elif isinstance(source, Path) or (isinstance(source, str) and not source.lstrip().startswith("{")):
        graphs = json.loads(Path(source).read_text())
    else:
        graphs = json.loads(source)
    version = graphs.get("version")
    if version != GRAPHS_VERSION:
        raise ValueError(f"graphs version {version!r} is not supported; expected {GRAPHS_VERSION}")
    if not graphs.get("actions"):
        raise ValueError(f"graphs export of {graphs.get('subject')!r} holds no action graph")
    return graphs


def literal(text: str | None) -> Any:
    """The Python value of a literal written in the model, or the text itself when it is none."""
    if text is None:
        return None
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1]
    if text in ("true", "false"):
        return text == "true"
    for convert in (int, float):
        try:
            return convert(text)
        except ValueError:
            continue
    return text


def _attribute_values(form: Mapping[str, Any]) -> dict[str, Any]:
    parameters = {p["name"] for p in form.get("parameters") or ()}
    values: dict[str, Any] = {}
    for attribute in form.get("attributes") or ():
        if attribute["name"] in parameters:
            continue
        value = attribute.get("value")
        values[attribute["name"]] = literal(value["text"]) if value else None
    return values


def _pins(form: Mapping[str, Any], direction: str) -> dict[str, str]:
    return {
        p["name"]: (p.get("types") or [""])[0]
        for p in form.get("parameters") or ()
        if p.get("direction") == direction
    }


class SysMLActionDag:
    """The steps and dependencies of an action subject, ready to become Airflow tasks.

    :param graphs: A ``graphs:1`` export: the JSON text, a path to a file the CLI
        wrote, or the parsed mapping
    :param task_factory: Builds the operator of each step; :class:`KubernetesPodFactory` by default
    """

    def __init__(self, graphs: str | Path | Mapping[str, Any], *, task_factory: TaskFactory | None = None):
        self.graphs = load_graphs(graphs)
        self.subject: str = self.graphs["subject"]
        self.task_factory: TaskFactory = task_factory or KubernetesPodFactory()
        forms = {form["name"]: form for form in self.graphs["actions"]}
        subject_form = forms.get(self.subject) or self.graphs["actions"][0]
        self.steps: list[ActionStep] = _steps(subject_form, forms)
        self.dependencies: list[tuple[str, str]] = _dependencies(subject_form, self.steps)

    @classmethod
    def from_file(cls, path: str | Path, **kwargs: Any) -> SysMLActionDag:
        """From a ``graphs.json`` the CLI wrote: ``sysml model.sysml -graphs <subject> -o graphs.json``."""
        return cls(Path(path), **kwargs)

    @classmethod
    def from_model(
        cls,
        model_path: str,
        subject: str,
        *,
        opensysml_conn_id: str = "opensysml_default",
        strict: bool = True,
        **kwargs: Any,
    ) -> SysMLActionDag:
        """Export the subject live through :class:`~airflow_provider_opensysml.hooks.OpenSysMLHook`."""
        from airflow_provider_opensysml.hooks import OpenSysMLHook

        hook = OpenSysMLHook(opensysml_conn_id)
        connection = hook.get_conn()
        try:
            model = hook.load(connection, model_path, strict=strict)
            graphs = model.export_graphs(subject)
        finally:
            connection.close()
        if graphs.version != GRAPHS_VERSION:
            raise ValueError(f"graphs version {graphs.version} is not supported; expected {GRAPHS_VERSION}")
        return cls(graphs.content, **kwargs)

    @property
    def task_ids(self) -> list[str]:
        return [step.task_id for step in self.steps]

    def step(self, task_id: str) -> ActionStep:
        for step in self.steps:
            if step.task_id == task_id:
                return step
        raise KeyError(task_id)

    def build(self, dag: DAG) -> dict[str, BaseOperator]:
        """Add one task per step to ``dag`` and wire the dependencies; returns the tasks by id."""
        tasks = {step.task_id: self.task_factory(step, dag) for step in self.steps}
        for upstream, downstream in self.dependencies:
            tasks[upstream] >> tasks[downstream]
        return tasks

    def dag(self, dag_id: str, **dag_kwargs: Any) -> DAG:
        """A new DAG holding the steps."""
        from airflow.sdk import DAG

        dag = DAG(dag_id=dag_id, **dag_kwargs)
        with dag:
            self.build(dag)
        return dag


def _steps(subject_form: Mapping[str, Any], forms: Mapping[str, Mapping[str, Any]]) -> list[ActionStep]:
    nodes = subject_form.get("nodes") or ()
    names = _task_names(nodes)
    flows_in: dict[int, list[Flow]] = {}
    flows_out: dict[int, list[Flow]] = {}
    for flow in subject_form.get("flows") or ():
        if flow["source"] not in names or flow["target"] not in names:
            continue
        item = Flow(
            names[flow["source"]], flow.get("sourcePin", ""), names[flow["target"]], flow.get("targetPin", "")
        )
        flows_out.setdefault(flow["source"], []).append(item)
        flows_in.setdefault(flow["target"], []).append(item)

    fallback = _attribute_values(subject_form)
    steps = []
    for node in nodes:
        if node["id"] not in names:
            continue
        performs = tuple(node.get("performs") or ())
        behavior = forms.get(performs[0]) if performs else None
        attributes = dict(fallback)
        if behavior is not None:
            attributes.update(_attribute_values(behavior))
        steps.append(
            ActionStep(
                task_id=names[node["id"]],
                kind=node["kind"],
                performs=performs,
                attributes=attributes,
                inputs=_pins(behavior, "in") if behavior else {},
                outputs=_pins(behavior, "out") if behavior else {},
                flows_in=tuple(flows_in.get(node["id"], ())),
                flows_out=tuple(flows_out.get(node["id"], ())),
            )
        )
    return steps


def _task_names(nodes: Iterable[Mapping[str, Any]]) -> dict[int, str]:
    names: dict[int, str] = {}
    for node in nodes:
        kind = node["kind"]
        if kind in CONTROL_NODE_KINDS:
            continue
        if kind not in TASK_NODE_KINDS:
            raise ValueError(f"node {node.get('name') or node['id']} of kind {kind!r} cannot become a task")
        name = node.get("name")
        if not name:
            raise ValueError(f"action node {node['id']} has no name to use as a task id")
        if name in names.values():
            raise ValueError(f"two action nodes are both named {name!r}")
        names[node["id"]] = name
    return names


def _dependencies(subject_form: Mapping[str, Any], steps: list[ActionStep]) -> list[tuple[str, str]]:
    nodes = {node["id"]: node for node in subject_form.get("nodes") or ()}
    names = {node["id"]: node["name"] for node in nodes.values() if node["kind"] in TASK_NODE_KINDS}
    incoming: dict[int, list[int]] = {}
    for edge in subject_form.get("edges") or ():
        incoming.setdefault(edge["target"], []).append(edge["source"])

    def producers(node_id: int, seen: set[int]) -> list[int]:
        found: list[int] = []
        for source in incoming.get(node_id, ()):
            if source in names:
                found.append(source)
            elif source not in seen:
                seen.add(source)
                found.extend(producers(source, seen))
        return found

    edges: set[tuple[str, str]] = set()
    for node_id, name in names.items():
        for source in producers(node_id, set()):
            edges.add((names[source], name))
    for step in steps:
        for flow in step.flows_in:
            if flow.producer != step.task_id:
                edges.add((flow.producer, step.task_id))
    order = {step.task_id: index for index, step in enumerate(steps)}
    return sorted(_transitive_reduction(edges), key=lambda edge: (order[edge[0]], order[edge[1]]))


def _transitive_reduction(edges: set[tuple[str, str]]) -> set[tuple[str, str]]:
    successors: dict[str, set[str]] = {}
    for upstream, downstream in edges:
        successors.setdefault(upstream, set()).add(downstream)

    def reachable(start: str) -> set[str]:
        seen: set[str] = set()
        stack = list(successors.get(start, ()))
        while stack:
            node = stack.pop()
            if node not in seen:
                seen.add(node)
                stack.extend(successors.get(node, ()))
        return seen

    return {
        (upstream, downstream)
        for upstream, downstream in edges
        if not any(downstream in reachable(other) for other in successors[upstream] if other != downstream)
    }


def empty_task_factory(step: ActionStep, dag: DAG) -> BaseOperator:
    """An :class:`~airflow.providers.standard.operators.empty.EmptyOperator` per step, for tests."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    return EmptyOperator(task_id=step.task_id, dag=dag)


def conf_arguments(step: ActionStep) -> list[str]:
    """One ``{{ dag_run.conf['<task_id>.<pin>'] }}`` argument per pin, inputs then outputs."""
    return [f"{{{{ dag_run.conf['{step.task_id}.{pin}'] }}}}" for pin in (*step.inputs, *step.outputs)]


@dataclass
class KubernetesPodFactory:
    """Runs each step as a ``KubernetesPodOperator``.

    The pod runs ``cmds`` with ``<wrapper_dir>/<wrapper>`` and the step's
    arguments, in the ``image`` attribute of the behavior or subject, requesting
    and limited to ``cpu`` cores and ``memoryGi`` GiB. ``arguments`` maps a step
    to the arguments after the wrapper; by default every pin is read from
    ``dag_run.conf`` as ``<task_id>.<pin>``. ``pod_kwargs`` are passed through
    to every operator (namespace, env_vars, volumes, ...).
    """

    arguments: Callable[[ActionStep], list[str]] = conf_arguments
    cmds: list[str] = field(default_factory=lambda: ["/bin/bash"])
    wrapper_dir: str = "/opt/wrappers"
    image_attribute: str = "image"
    wrapper_attribute: str = "wrapper"
    cpu_attribute: str = "cpu"
    memory_attribute: str = "memoryGi"
    pod_kwargs: dict[str, Any] = field(default_factory=dict)

    def __call__(self, step: ActionStep, dag: DAG) -> BaseOperator:
        from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
        from kubernetes.client import models as k8s

        image = step.attributes.get(self.image_attribute)
        wrapper = step.attributes.get(self.wrapper_attribute)
        if not image or not wrapper:
            needed = f"{self.image_attribute!r} and {self.wrapper_attribute!r}"
            raise ValueError(f"step {step.task_id} needs the attributes {needed}")
        resources = self.resources(step)
        kwargs: dict[str, Any] = {"name": step.task_id.replace("_", "-"), **self.pod_kwargs}
        return KubernetesPodOperator(
            task_id=step.task_id,
            image=image,
            cmds=list(self.cmds),
            arguments=[f"{self.wrapper_dir}/{wrapper}", *self.arguments(step)],
            container_resources=k8s.V1ResourceRequirements(requests=resources, limits=resources),
            dag=dag,
            **kwargs,
        )

    def resources(self, step: ActionStep) -> dict[str, str]:
        resources: dict[str, str] = {}
        if (cpu := step.attributes.get(self.cpu_attribute)) is not None:
            resources["cpu"] = str(cpu)
        if (memory := step.attributes.get(self.memory_attribute)) is not None:
            resources["memory"] = f"{memory}Gi"
        return resources
