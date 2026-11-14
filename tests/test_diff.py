"""Diff-engine tests: classification, tightening direction, rename detection."""

from __future__ import annotations

import pytest

from dcp.contract import parse_contract
from dcp.diff import diff_contracts, detect_renames


def make(columns, **over):
    doc = {
        "contract_id": "s.t",
        "upstream_system": "s",
        "table": "t",
        "columns": columns,
        "primary_key": over.pop("primary_key", ["id"]),
        **over,
    }
    return parse_contract(doc, version=over.pop("version", 1))


BASE = [
    {"name": "id", "type": "string"},
    {"name": "amount", "type": "decimal", "minimum": 0, "maximum": 100, "precision": 10, "scale": 2},
    {"name": "status", "type": "string", "enum": ["a", "b"]},
    {"name": "created_at", "type": "timestamp"},
]


def kinds(cs):
    return sorted({c.kind for c in cs.changes})


def test_identical_contracts_have_no_changes():
    old = make(BASE)
    new = make(BASE, version=2)
    cs = diff_contracts(old, new)
    assert cs.compatibility == "no_change"


def test_add_nullable_is_compatible():
    old = make(BASE)
    new = make(BASE + [{"name": "extra", "type": "string", "required": False}], version=2)
    cs = diff_contracts(old, new)
    assert cs.compatibility == "backward_compatible"


def test_add_required_is_breaking():
    old = make(BASE)
    new = make(BASE + [{"name": "extra", "type": "string"}], version=2)
    cs = diff_contracts(old, new)
    assert cs.compatibility == "breaking"
    assert cs.requires_signoff


def test_drop_column_is_breaking():
    old = make(BASE)
    new = make([c for c in BASE if c["name"] != "status"], version=2)
    cs = diff_contracts(old, new)
    assert any(c.kind == "drop_column" for c in cs.changes)
    assert cs.breaking


@pytest.mark.parametrize("old_t,new_t,breaking", [
    ("int", "bigint", False),
    ("int", "decimal", False),
    ("date", "timestamp", False),
    ("bigint", "int", True),
    ("decimal", "int", True),
    ("string", "int", True),
])
def test_type_change_direction(old_t, new_t, breaking):
    old = make([{"name": "id", "type": "string"}, {"name": "v", "type": old_t}])
    new = make([{"name": "id", "type": "string"}, {"name": "v", "type": new_t}], version=2)
    cs = diff_contracts(old, new)
    assert any(c.kind == "type_change" for c in cs.changes)
    assert cs.compatibility == ("breaking" if breaking else "backward_compatible")


def test_enum_add_is_compatible_remove_is_breaking():
    old = make(BASE)
    add = make([{**c, "enum": ["a", "b", "c"]} if c["name"] == "status" else c for c in BASE], version=2)
    assert diff_contracts(old, add).compatibility == "backward_compatible"
    remove = make([{**c, "enum": ["a"]} if c["name"] == "status" else c for c in BASE], version=2)
    assert diff_contracts(old, remove).compatibility == "breaking"


def test_range_widen_compatible_tighten_breaking():
    old = make(BASE)
    widen = make([{**c, "maximum": 1000} if c["name"] == "amount" else c for c in BASE], version=2)
    assert diff_contracts(old, widen).compatibility == "backward_compatible"
    tighten = make([{**c, "maximum": 50} if c["name"] == "amount" else c for c in BASE], version=2)
    assert diff_contracts(old, tighten).compatibility == "breaking"


def test_required_tighten_breaking_relax_compatible():
    cols = [dict(c) for c in BASE]
    cols[3]["required"] = False
    old = make(cols)
    new = make(BASE, version=2)
    cs = diff_contracts(old, new)
    assert any(c.kind == "required_change" and c.breaking for c in cs.changes)


def test_unique_addition_is_breaking():
    old = make(BASE)
    new = make([{**c, "unique": True} if c["name"] == "created_at" else c for c in BASE], version=2)
    assert diff_contracts(old, new).compatibility == "breaking"


def test_precision_reduction_is_breaking():
    old = make(BASE)
    new = make([{**c, "scale": 1} if c["name"] == "amount" else c for c in BASE], version=2)
    assert diff_contracts(old, new).compatibility == "breaking"


def test_freshness_equivalent_spelling_is_no_change():
    old = make(BASE, freshness={"max_age": "PT24H", "column": "created_at"})
    new = make(BASE, version=2, freshness={"max_age": "P1D", "column": "created_at"})
    cs = diff_contracts(old, new)
    assert cs.compatibility == "backward_compatible"
    assert not cs.breaking


def test_freshness_tighten_is_breaking():
    old = make(BASE, freshness={"max_age": "P1D", "column": "created_at"})
    new = make(BASE, version=2, freshness={"max_age": "PT1H", "column": "created_at"})
    assert diff_contracts(old, new).compatibility == "breaking"


def test_rename_detected_from_names():
    old = make(BASE)
    new = make([{**c, "name": "order_amount"} if c["name"] == "amount" else c for c in BASE], version=2)
    cs = diff_contracts(old, new)
    assert cs.renames and cs.renames[0]["old_name"] == "amount"
    assert cs.renames[0]["new_name"] == "order_amount"
    assert any(c.kind == "rename_column" for c in cs.changes)


def test_rename_of_similar_name_is_not_drop_add():
    old = make(BASE)
    new = make([{**c, "name": "created_time"} if c["name"] == "created_at" else c for c in BASE], version=2)
    cs = diff_contracts(old, new)
    assert not any(c.kind == "drop_column" for c in cs.changes)
    assert not any(c.kind == "add_column" for c in cs.changes)


def test_rename_detected_from_sample_values_even_with_unrelated_name():
    old = make(BASE)
    new = make([{**c, "name": "gmt_create"} if c["name"] == "created_at" else c for c in BASE], version=2)
    old_prof = {"created_at": {"sample_values": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"]}}
    new_prof = {"gmt_create": {"sample_values": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"]}}
    cs = diff_contracts(old, new, old_profiles=old_prof, new_profiles=new_prof)
    assert ("created_at", "gmt_create") in [(r["old_name"], r["new_name"]) for r in cs.renames]


def test_conflicting_sample_values_veto_rename():
    old = make(BASE)
    new = make([{**c, "name": "created_at_hash"} if c["name"] == "created_at" else c for c in BASE], version=2)
    old_prof = {"created_at": {"sample_values": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"]}}
    new_prof = {"created_at_hash": {"sample_values": ["deadbeef", "cafebabe"]}}
    cs = diff_contracts(old, new, old_profiles=old_prof, new_profiles=new_prof)
    assert not cs.renames


def test_explicit_renamed_from_hint_is_honoured():
    old = make(BASE)
    new = make(
        [{**c, "name": "gmt_create", "renamed_from": "created_at"} if c["name"] == "created_at" else c for c in BASE],
        version=2,
    )
    cs = diff_contracts(old, new)
    assert any(r["new_name"] == "gmt_create" and r["confidence"] == 1.0 for r in cs.renames)


def test_primary_key_change_is_breaking():
    old = make(BASE)
    new = make(BASE, version=2, primary_key=["id", "created_at"])
    cs = diff_contracts(old, new)
    assert any(c.kind == "primary_key_change" and c.breaking for c in cs.changes)


def test_rule_added_is_breaking():
    old = make(BASE)
    new = make(BASE, version=2, rules=[{"name": "r", "kind": "range", "column": "amount", "minimum": 0}])
    assert diff_contracts(old, new).compatibility == "breaking"


def test_renames_are_deterministic_and_greedy():
    old = make(BASE)
    new = make(
        [
            {**c, "name": "created_time"} if c["name"] == "created_at" else
            {**c, "name": "order_amount"} if c["name"] == "amount" else c
            for c in BASE
        ],
        version=2,
    )
    cs = diff_contracts(old, new)
    pairs = {(r["old_name"], r["new_name"]) for r in cs.renames}
    assert ("amount", "order_amount") in pairs
    assert ("created_at", "created_time") in pairs


def test_initial_contract_has_single_created_change():
    new = make(BASE)
    cs = diff_contracts(None, new)
    assert len(cs.changes) == 1
    assert cs.changes[0].kind == "contract_created"
    assert cs.compatibility == "backward_compatible"
