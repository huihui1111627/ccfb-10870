"""Validator tests: type checks, constraints, sampling, freshness, drilldown."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dcp.contract import parse_contract
from dcp.validator import (
    parse_duration,
    run_validation,
    structural_issues,
    validate_rows,
    validate_submission_sample,
    UpstreamData,
)


def contract(**over):
    doc = {
        "contract_id": "s.t",
        "upstream_system": "s",
        "table": "t",
        "columns": [
            {"name": "id", "type": "string", "unique": True},
            {"name": "amount", "type": "decimal", "minimum": 0, "maximum": 1000, "precision": 8, "scale": 2},
            {"name": "status", "type": "string", "enum": ["paid", "refunded"]},
            {"name": "note", "type": "string", "required": False},
            {"name": "created_at", "type": "timestamp"},
        ],
        "primary_key": ["id"],
    }
    doc.update(over)
    return parse_contract(doc, version=1)


def row(**kw):
    base = {
        "id": "1",
        "amount": 10.5,
        "status": "paid",
        "note": None,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    base.update(kw)
    return base


def rules(result):
    return sorted({v.rule_name for v in result.violations})


def test_clean_rows_pass():
    res = validate_rows(contract(), [row(), row(id="2")])
    assert res.violation_count == 0
    assert res.rows_clean == 2


def test_null_in_required_column():
    res = validate_rows(contract(), [row(amount=None)])
    assert "column.amount.not_null" in rules(res)
    assert res.violations[0].row_pk == {"id": "1"}


def test_null_in_nullable_column_is_fine():
    res = validate_rows(contract(), [row(note=None)])
    assert res.violation_count == 0


def test_enum_violation():
    res = validate_rows(contract(), [row(status="unknown")])
    assert "column.status.enum" in rules(res)


def test_range_violations_both_sides():
    lo = validate_rows(contract(), [row(amount=-1)])
    hi = validate_rows(contract(), [row(amount=1001)])
    assert "column.amount.min" in rules(lo)
    assert "column.amount.max" in rules(hi)


def test_boundary_values_are_accepted():
    res = validate_rows(contract(), [row(amount=0), row(id="2", amount=1000)])
    assert res.violation_count == 0


def test_bool_stored_as_int_is_accepted():
    c = parse_contract({
        "contract_id": "s.t", "upstream_system": "s", "table": "t",
        "columns": [{"name": "flag", "type": "bool"}],
    })
    assert validate_rows(c, [{"flag": 1}, {"flag": 0}]).violation_count == 0
    assert validate_rows(c, [{"flag": 2}]).violation_count == 1


def test_numeric_string_is_type_violation():
    res = validate_rows(contract(), [row(amount="10.5")])
    assert "column.amount.type" in rules(res)


def test_decimal_scale_overflow_is_flagged():
    res = validate_rows(contract(), [row(amount=10.555)])
    assert "column.amount.type" in rules(res)


def test_decimal_precision_overflow_is_flagged():
    res = validate_rows(contract(), [row(amount=1234567.0)])
    assert "column.amount.type" in rules(res)


def test_duplicate_primary_key():
    res = validate_rows(contract(), [row(), row()])
    assert "table.primary_key" in rules(res)


def test_duplicate_unique_column():
    c = parse_contract({
        "contract_id": "s.t", "upstream_system": "s", "table": "t",
        "columns": [{"name": "id", "type": "string"}, {"name": "email", "type": "string", "unique": True}],
        "primary_key": ["id"],
    })
    res = validate_rows(c, [{"id": "1", "email": "a@x"}, {"id": "2", "email": "a@x"}])
    assert "column.email.unique" in rules(res)


def test_nulls_in_unique_column_are_not_duplicates():
    c = parse_contract({
        "contract_id": "s.t", "upstream_system": "s", "table": "t",
        "columns": [{"name": "id", "type": "string"}, {"name": "email", "type": "string", "unique": True, "required": False}],
        "primary_key": ["id"],
    })
    res = validate_rows(c, [{"id": "1", "email": None}, {"id": "2", "email": None}])
    assert res.violation_count == 0


def test_composite_unique_key():
    c = parse_contract({
        "contract_id": "s.t", "upstream_system": "s", "table": "t",
        "columns": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}],
        "unique_keys": [["a", "b"]],
    })
    res = validate_rows(c, [{"a": "1", "b": "2"}, {"a": "1", "b": "2"}])
    assert any("unique_key" in r for r in rules(res))


def test_expression_rule_failure():
    c = contract(rules=[{"name": "pos", "kind": "expression", "expression": "amount >= 0"}])
    res = validate_rows(c, [row(amount=-5)])
    assert "pos" in rules(res)


def test_expression_with_null_does_not_crash():
    c = contract(rules=[{"name": "paid_after", "kind": "expression",
                         "expression": "note is null or created_at >= note"}])
    res = validate_rows(c, [row(note=None)])
    assert res.violation_count == 0


def test_expression_type_mismatch_counts_as_violation():
    c = contract(rules=[{"name": "r", "kind": "expression", "expression": "amount >= created_at"}])
    # comparing a number with a string is a data problem, not a crash
    res = validate_rows(c, [row()])
    assert "r" in rules(res)


def test_row_count_rule():
    c = contract(rules=[{"name": "rc", "kind": "row_count", "minimum": 10}])
    res = validate_rows(c, [row()])
    assert "rc" in rules(res)


def test_null_ratio_rule():
    c = contract(rules=[{"name": "nr", "kind": "null_ratio", "column": "note", "maximum": 0.1}])
    res = validate_rows(c, [row(note=None)] * 5)
    assert "nr" in rules(res)


def test_freshness_stale():
    c = contract(freshness={"max_age": "PT1H", "column": "created_at"})
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    res = validate_rows(c, [row(created_at=old)])
    assert "freshness" in rules(res)


def test_freshness_satisfied():
    c = contract(freshness={"max_age": "PT1H", "column": "created_at"})
    res = validate_rows(c, [row()])
    assert "freshness" not in rules(res)


def test_structural_issues():
    issues = structural_issues(contract(), ["id", "amount", "status", "note", "extra"])
    assert any("created_at" in i for i in issues)
    assert any("extra" in i for i in issues)


def test_violations_carry_row_snapshot_for_drilldown():
    res = validate_rows(contract(), [row(amount=-1)])
    v = res.violations[0]
    assert v.row_pk == {"id": "1"}
    assert v.row_data["status"] == "paid"


def test_max_violations_per_rule_caps_storage():
    rows = [row(id=str(i), amount=-1) for i in range(100)]
    res = validate_rows(contract(), rows, max_violations_per_rule=5)
    stored = [v for v in res.violations if v.rule_name == "column.amount.min"]
    assert len(stored) == 5
    # true count still reflects all offending rows
    assert res.by_rule["column.amount.min"] == 100


def test_deterministic_sampling():
    c = contract()
    many = [row(id=str(i)) for i in range(500)]
    a = run_validation(c, UpstreamData(cols := list(many[0]), many, True, 500))
    b = run_validation(c, UpstreamData(cols, many, True, 500))
    assert a.rows_checked == b.rows_checked


def test_submission_sample_rejects_undeclared_columns():
    errs = validate_submission_sample(contract(), [row(extra_col=1)])
    assert any("undeclared" in e for e in errs)


def test_submission_sample_rejects_missing_columns():
    errs = validate_submission_sample(contract(), [{"id": "1"}])
    assert any("missing columns" in e for e in errs)


def test_submission_sample_rejects_contract_violations():
    errs = validate_submission_sample(contract(), [row(amount=-1)])
    assert errs and "sample violates" in errs[0]


def test_submission_sample_accepts_clean_rows():
    assert validate_submission_sample(contract(), [row()]) == []


def test_submission_sample_requires_rows():
    assert validate_submission_sample(contract(), []) == ["submission sample is empty"]


def test_padded_timestamp_is_not_a_type_error():
    res = validate_rows(contract(), [row(created_at="  2024-01-01T00:00:00+00:00  ")])
    assert res.violation_count == 0


def test_padded_enum_value_is_accepted():
    res = validate_rows(contract(), [row(status="  paid  ")])
    assert res.violation_count == 0


def test_padded_date_is_accepted():
    c = parse_contract({
        "contract_id": "s.t", "upstream_system": "s", "table": "t",
        "columns": [{"name": "d", "type": "date"}],
    })
    assert validate_rows(c, [{"d": " 2024-01-01 "}]).violation_count == 0


def test_parse_duration():
    assert parse_duration("PT1H").total_seconds() == 3600
    assert parse_duration("P1D").total_seconds() == 86400
    assert parse_duration("P1DT2H30M").total_seconds() == 86400 * 1 + 9000
    assert parse_duration("garbage") is None


def test_invalid_timestamp_is_type_violation():
    res = validate_rows(contract(), [row(created_at="not-a-time")])
    assert "column.created_at.type" in rules(res)


def test_date_column_rejects_timestamp():
    c = parse_contract({
        "contract_id": "s.t", "upstream_system": "s", "table": "t",
        "columns": [{"name": "d", "type": "date"}],
    })
    assert validate_rows(c, [{"d": "2024-01-01"}]).violation_count == 0
    assert validate_rows(c, [{"d": "2024-01-01T10:00:00"}]).violation_count == 1


def test_sampled_flag_and_total():
    c = contract()
    res = run_validation(c, UpstreamData(list(row()), [row()], sampled=True, total=5000))
    assert res.sampled is True
    assert res.rows_total == 5000
