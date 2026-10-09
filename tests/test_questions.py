from __future__ import annotations

from types import SimpleNamespace

import pytest

from airflow_provider_opensysml.questions import QUESTION_KINDS, Answer, SysMLQuestion


def verdict(**overrides):
    base = dict(
        kind="requirement",
        element="R::soft",
        element_id="R::soft",
        holds=True,
        status="holds",
        question="evaluate",
        condition="",
        error="",
        requirement_id="",
        instance_path="",
        verifications=[],
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def analysis(verdicts=(), verifications=(), evaluations=()):
    return SimpleNamespace(
        outputs={},
        verdicts=list(verdicts),
        verifications=list(verifications),
        evaluations=list(evaluations),
        standing=SimpleNamespace(engine="e", strength="s", bounds=[]),
    )


class FakeModel:
    def __init__(self, **answers):
        self.answers = answers
        self.calls = []

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return self.answers[name]

        return call


def test_kinds_and_argument_checks():
    assert QUESTION_KINDS == ("constraint", "requirement", "satisfy", "object", "case")
    with pytest.raises(ValueError):
        SysMLQuestion("bogus", "E")
    with pytest.raises(ValueError):
        SysMLQuestion("constraint")
    with pytest.raises(ValueError):
        SysMLQuestion("object", "E", subject="S")
    with pytest.raises(ValueError):
        SysMLQuestion("requirement", "E", named_arguments={"x": 1})
    with pytest.raises(ValueError):
        SysMLQuestion("case", "C", question="holds")
    assert str(SysMLQuestion("satisfy")) == "every satisfaction"
    assert str(SysMLQuestion("requirement", "R::soft")) == "requirement R::soft"


def test_round_trips_through_its_dict():
    question = SysMLQuestion("case", "V::check", subject="V::w", named_arguments={"limit": 2})
    assert SysMLQuestion(**question.as_dict()) == question


def test_requirement_holds():
    model = FakeModel(verify_requirement=verdict())
    answer = SysMLQuestion("requirement", "R::soft", engine="smt", question="holds").ask(model)
    assert answer == Answer(True, answer.report)
    assert answer.report["holds"] is True and answer.report["kind"] == "requirement"
    assert model.calls == [
        ("verify_requirement", ("R::soft",), {"subject": None, "engine": "smt", "question": "holds"})
    ]


def test_requirement_does_not_hold():
    model = FakeModel(verify_requirement=verdict(holds=False, condition="speed <= limit"))
    answer = SysMLQuestion("requirement", "R::soft").ask(model)
    assert not answer.holds and answer.decided
    assert answer.failures == ["requirement R::soft does not hold (speed <= limit)"]


def test_requirement_undecided_is_an_error_not_a_false():
    model = FakeModel(verify_requirement=verdict(holds=False, error="no engine covers it"))
    answer = SysMLQuestion("requirement", "R::soft").ask(model)
    assert not answer.holds and not answer.decided
    assert answer.error == "requirement R::soft could not be decided: no engine covers it"
    assert answer.failures == []


def test_other_verify_kinds_call_their_methods():
    model = FakeModel(
        verify_constraint=verdict(kind="constraint"),
        verify_satisfaction=verdict(kind="satisfy"),
        validate_instance=verdict(kind="object"),
    )
    assert SysMLQuestion("constraint", "C", subject="S").ask(model).holds
    assert SysMLQuestion("satisfy").ask(model).holds
    assert SysMLQuestion("object", "O").ask(model).holds
    assert [c[0] for c in model.calls] == ["verify_constraint", "verify_satisfaction", "validate_instance"]
    assert model.calls[1] == ("verify_satisfaction", (None,), {"engine": None})


def test_case_binds_arguments_and_reads_verdicts():
    passed = SimpleNamespace(case_id="V::check", kind="pass", detail="", subcase=False, requirement_id="")
    model = FakeModel(run_analysis=analysis([verdict(kind="objective", element="obj")], [passed]))
    question = SysMLQuestion("case", "V::check", named_arguments={"limit": 2.0}, subject="V::w")
    answer = question.ask(model)
    assert answer.holds
    (call,) = model.calls
    assert call[1] == ("V::check",)
    assert call[2]["named_arguments"] == {"limit": 2.0} and call[2]["subject"] == "V::w"
    assert answer.report["verifications"][0]["kind"] == "pass"


def test_case_failures_and_errors():
    failed = SimpleNamespace(case_id="V::check", kind="fail", detail="", subcase=False, requirement_id="")
    model = FakeModel(
        run_analysis=analysis(
            [verdict(kind="objective", element="obj", holds=False, condition="a == b")], [failed]
        )
    )
    answer = SysMLQuestion("case", "V::check").ask(model)
    assert not answer.holds and answer.decided
    assert answer.failures == [
        "V::check: objective obj does not hold (a == b); verification V::check verdict: fail"
    ]

    unbound = SimpleNamespace(
        case_id="V::check", kind="error", detail="unbound parameter", subcase=False, requirement_id=""
    )
    answer = SysMLQuestion("case", "V::check").ask(FakeModel(run_analysis=analysis([], [unbound])))
    assert not answer.decided and "unbound parameter" in answer.error

    broken = analysis([verdict(kind="objective", element="obj", holds=False, error="no solver")])
    answer = SysMLQuestion("case", "V::check").ask(FakeModel(run_analysis=broken))
    assert answer.error == "V::check: objective obj could not be decided: no solver"


def test_a_question_the_model_cannot_take_is_undecided_not_raised():
    from opensysml.errors import ConnectionError as OpenSysMLConnectionError
    from opensysml.errors import SymbolNotFoundError

    class Refusing(FakeModel):
        def verify_requirement(self, *args, **kwargs):
            raise SymbolNotFoundError("R::missing")

    answer = SysMLQuestion("requirement", "R::missing").ask(Refusing())
    assert not answer.decided and not answer.holds
    assert "requirement R::missing could not be decided" in answer.error and "R::missing" in answer.error

    class Unreachable(FakeModel):
        def verify_requirement(self, *args, **kwargs):
            raise OpenSysMLConnectionError("service gone")

    with pytest.raises(OpenSysMLConnectionError):
        SysMLQuestion("requirement", "R::x").ask(Unreachable())
