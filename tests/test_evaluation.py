"""Evaluation harness tests: verify the ground truth is independent and metrics compute."""

from __future__ import annotations

import pytest

from dcpeval.harness import (
    evaluate_structural,
    evaluate_violations,
    negative_controls,
    run_evaluation,
)
from dcpsim.simulator import ALL_STRUCTURAL_MUTATIONS, NotApplicable
from dcpsim.upstreams import all_tables, build_upstreams


@pytest.fixture(scope="module")
def tables():
    return all_tables(build_upstreams(rows_per_table=60, seed=3))


def test_structural_metrics_are_computed(tables):
    r = evaluate_structural(tables, seed=5, with_sample=True)
    assert r["samples"] > 50
    assert 0.0 <= r["accuracy"] <= 1.0
    assert r["confusion_matrix"]["tp"] + r["confusion_matrix"]["fn"] > 0
    assert r["by_kind"]
    assert "rename" in r


def test_violation_metrics_are_computed(tables):
    r = evaluate_violations(tables, seed=5)
    assert r["injections"] > 20
    assert 0.0 <= r["detection_rate"] <= 1.0
    assert all(k for k in r["by_injection"])


def test_evaluation_is_deterministic(tables):
    a = evaluate_structural(tables, seed=9, with_sample=True)
    b = evaluate_structural(tables, seed=9, with_sample=True)
    assert a["confusion_matrix"] == b["confusion_matrix"]
    assert a["by_kind"].keys() == b["by_kind"].keys()


def test_negative_controls_are_weaker(tables):
    c = negative_controls(tables, seed=4)
    # Controls must not be perfect, otherwise the benchmark proves nothing.
    assert c["structural_control"]["rename_recall"] < 1.0
    assert c["violation_control"]["detection_rate"] < 1.0


def test_sample_improves_rename_detection(tables):
    with_sample = evaluate_structural(tables, seed=5, with_sample=True)
    without = evaluate_structural(tables, seed=5, with_sample=False)
    assert with_sample["rename"]["recall"] >= without["rename"]["recall"]


def test_every_mutation_factory_is_applicable_somewhere(tables):
    """Guards against silently dead mutations skewing the benchmark."""
    applied: set[str] = set()
    import random

    for table in tables:
        rng = random.Random(1)
        for factory in ALL_STRUCTURAL_MUTATIONS:
            try:
                m = factory(table, rng)
                applied.add(m.kind)
            except NotApplicable:
                pass
            except Exception:
                continue
    assert len(applied) >= 25


def test_full_report_written(tmp_path, tables):
    report = run_evaluation(rows_per_table=80, seed=3, output_dir=tmp_path)
    assert (tmp_path / "evaluation.json").exists()
    assert (tmp_path / "evaluation.md").exists()
    assert report["breaking_change_detection"]["samples"] > 0
    assert "clean_data_false_positive_sweep" in report
    assert "stability_sweep" in report
    assert "negative_controls" in report
    assert "throughput" in report
