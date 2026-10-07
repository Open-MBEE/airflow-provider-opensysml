"""Plain-data forms of the ``opensysml`` client's answers, for XCom."""

from __future__ import annotations

from typing import Any


def jsonable(value: Any) -> Any:
    """``value`` as data XCom can carry: primitives as they are, client objects by their identity."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [jsonable(v) for v in value]
    if hasattr(value, "magnitude") and hasattr(value, "unit"):
        return {"magnitude": jsonable(value.magnitude), "unit": str(value.unit)}
    if hasattr(value, "literal_id"):
        return value.literal_id
    if hasattr(value, "type_symbol_id") and hasattr(value, "id"):
        return {"instance": value.id, "type": value.type_symbol_id}
    return str(value)


def verification_to_dict(verification: Any) -> dict[str, Any]:
    return {
        "case": verification.case_id,
        "kind": verification.kind,
        "detail": verification.detail,
        "subcase": bool(verification.subcase),
        "requirement": verification.requirement_id,
    }


def verdict_to_dict(verdict: Any) -> dict[str, Any]:
    return {
        "kind": verdict.kind,
        "element": verdict.element,
        "element_id": verdict.element_id,
        "holds": bool(verdict.holds),
        "status": verdict.status,
        "question": verdict.question,
        "condition": verdict.condition,
        "error": verdict.error,
        "requirement": verdict.requirement_id,
        "instance_path": verdict.instance_path,
        "verifications": [verification_to_dict(v) for v in verdict.verifications],
    }


def evaluation_to_dict(evaluation: Any) -> dict[str, Any]:
    return {
        "function": evaluation.function_id,
        "arguments": [jsonable(a) for a in evaluation.arguments],
        "result": jsonable(evaluation.result),
        "error": evaluation.error,
        "selected": bool(evaluation.selected),
        "tied": bool(evaluation.tied),
    }


def standing_to_dict(standing: Any) -> dict[str, Any]:
    return {
        "engine": standing.engine,
        "strength": standing.strength,
        "bounds": [
            {"name": b.name, "limit": jsonable(b.limit), "reached": bool(b.reached)}
            for b in (standing.bounds or [])
        ],
    }


def analysis_result_to_dict(result: Any) -> dict[str, Any]:
    return {
        "outputs": {name: jsonable(value) for name, value in result.outputs.items()},
        "verdicts": [verdict_to_dict(v) for v in result.verdicts],
        "verifications": [verification_to_dict(v) for v in result.verifications],
        "evaluations": [evaluation_to_dict(e) for e in result.evaluations],
        "standing": standing_to_dict(result.standing),
    }
