from __future__ import annotations

import json
from types import SimpleNamespace

from airflow_provider_opensysml.results import analysis_result_to_dict, jsonable, verdict_to_dict


def test_jsonable_primitives_and_containers():
    assert jsonable({"a": (1, 2.5, "s", None, True)}) == {"a": [1, 2.5, "s", None, True]}


def test_jsonable_client_objects():
    quantity = SimpleNamespace(magnitude=3.0, unit="kg")
    literal = SimpleNamespace(literal_id="Verdicts::pass")
    instance = SimpleNamespace(id="i1", type_symbol_id="Rover::Wheel", features={})
    assert jsonable(quantity) == {"magnitude": 3.0, "unit": "kg"}
    assert jsonable(literal) == "Verdicts::pass"
    assert jsonable(instance) == {"instance": "i1", "type": "Rover::Wheel"}
    assert jsonable(object()).startswith("<object")


def _verdict(**overrides):
    base = dict(
        kind="requirement",
        element="R::mass",
        element_id="R::mass",
        holds=True,
        status="holds",
        question="evaluate",
        condition="",
        error="",
        requirement_id="R::mass",
        instance_path="",
        verifications=[
            SimpleNamespace(
                case_id="R::check", kind="pass", detail="", subcase=False, requirement_id="R::mass"
            )
        ],
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_verdict_to_dict_is_json():
    report = verdict_to_dict(_verdict())
    assert report["holds"] is True
    assert report["verifications"] == [
        {"case": "R::check", "kind": "pass", "detail": "", "subcase": False, "requirement": "R::mass"}
    ]
    json.dumps(report)


def test_analysis_result_to_dict_is_json():
    result = SimpleNamespace(
        outputs={"mass": ("float", 730.0), "q": SimpleNamespace(magnitude=1, unit="m")},
        verdicts=[_verdict(kind="objective")],
        verifications=[],
        evaluations=[
            SimpleNamespace(
                function_id="F::cost",
                arguments=[SimpleNamespace(id="c1", type_symbol_id="F::Candidate")],
                result=12.0,
                error="",
                selected=True,
                tied=False,
            )
        ],
        standing=SimpleNamespace(engine="run", strength="observed", bounds=[]),
    )
    report = analysis_result_to_dict(result)
    assert report["outputs"] == {"mass": ["float", 730.0], "q": {"magnitude": 1, "unit": "m"}}
    assert report["evaluations"][0]["arguments"] == [{"instance": "c1", "type": "F::Candidate"}]
    assert report["standing"] == {"engine": "run", "strength": "observed", "bounds": []}
    json.dumps(report)
