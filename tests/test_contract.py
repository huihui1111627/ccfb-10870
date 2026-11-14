"""Contract parsing and validation tests."""

from __future__ import annotations

import pytest

from dcp.contract import ContractError, parse_contract


def base_doc(**overrides):
    doc = {
        "contract_id": "order_core.orders",
        "upstream_system": "order_core",
        "table": "orders",
        "columns": [
            {"name": "order_id", "type": "string", "unique": True},
            {"name": "amount", "type": "decimal", "minimum": 0, "maximum": 1000, "precision": 10, "scale": 2},
            {"name": "status", "type": "string", "enum": ["paid", "refunded"]},
            {"name": "created_at", "type": "timestamp"},
        ],
        "primary_key": ["order_id"],
        "consumers": ["reporting.orders"],
        "owners": ["order_core.team"],
    }
    doc.update(overrides)
    return doc


def test_parses_minimal_contract():
    c = parse_contract(base_doc())
    assert c.contract_id == "order_core.orders"
    assert len(c.columns) == 4
    assert c.column("amount").minimum == 0
    assert c.schema_hash


def test_rejects_unknown_type():
    doc = base_doc(columns=[{"name": "a", "type": "varchar"}])
    with pytest.raises(ContractError, match="unknown type"):
        parse_contract(doc)


def test_rejects_duplicate_columns():
    doc = base_doc(columns=[{"name": "a", "type": "string"}, {"name": "a", "type": "string"}])
    with pytest.raises(ContractError, match="duplicate column"):
        parse_contract(doc)


def test_rejects_range_on_string():
    doc = base_doc(columns=[{"name": "a", "type": "string", "minimum": 1}])
    with pytest.raises(ContractError, match="ordered type"):
        parse_contract(doc)


def test_rejects_min_greater_than_max():
    doc = base_doc(columns=[{"name": "a", "type": "int", "minimum": 10, "maximum": 1}])
    with pytest.raises(ContractError, match="minimum is greater"):
        parse_contract(doc)


def test_rejects_enum_type_mismatch():
    doc = base_doc(columns=[{"name": "a", "type": "int", "enum": ["x"]}])
    with pytest.raises(ContractError, match="expected integer"):
        parse_contract(doc)


def test_rejects_unknown_primary_key():
    with pytest.raises(ContractError, match="unknown column"):
        parse_contract(base_doc(primary_key=["nope"]))


def test_rejects_invalid_rule_kind():
    doc = base_doc(rules=[{"name": "r", "kind": "sql_injection_clause", "column": "status"}])
    with pytest.raises(ContractError, match="unknown rule kind"):
        parse_contract(doc)


def test_rejects_malformed_contract_id():
    with pytest.raises(ContractError):
        parse_contract(base_doc(contract_id="!!!"))


def test_required_and_nullable_conflict():
    doc = base_doc(columns=[{"name": "a", "type": "string", "required": True, "nullable": True}])
    with pytest.raises(ContractError, match="disagree"):
        parse_contract(doc)


def test_physical_sql_type_does_not_change_hash():
    cols = [{"name": "order_id", "type": "string"}, {"name": "a", "type": "string"}]
    a = parse_contract(base_doc(columns=cols))
    b = parse_contract(base_doc(columns=[*cols[:1], {**cols[1], "sql_type": "VARCHAR(9)"}]))
    assert a.schema_hash == b.schema_hash


def test_enum_order_does_not_change_hash():
    cols = [{"name": "order_id", "type": "string"}, {"name": "a", "type": "string", "enum": ["x", "y"]}]
    a = parse_contract(base_doc(columns=cols))
    b = parse_contract(base_doc(columns=[cols[0], {**cols[1], "enum": ["y", "x"]}]))
    assert a.schema_hash == b.schema_hash


def test_freshness_rejects_unknown_column():
    with pytest.raises(ContractError, match="unknown column"):
        parse_contract(base_doc(freshness={"max_age": "PT1H", "column": "nope"}))


def test_rule_rejects_unknown_column():
    with pytest.raises(ContractError, match="unknown column"):
        parse_contract(base_doc(rules=[{"name": "r", "kind": "not_null", "column": "nope"}]))


def test_expression_rule_is_validated():
    tree_ok = base_doc(rules=[
        {"name": "paid", "kind": "expression", "expression": "amount >= 0"}
    ])
    assert parse_contract(tree_ok).rules[0].expression == "amount >= 0"
