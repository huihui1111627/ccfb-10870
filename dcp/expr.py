"""A deliberately small, safe boolean expression evaluator for contract rules.

Contract rules live in YAML written by upstream teams, so `eval` is out of the
question.  We parse with `ast` and only allow comparisons, boolean operators,
arithmetic on numbers/strings, and column references.  Function calls,
attribute access, subscripts and dunder names are rejected.
"""

from __future__ import annotations

import ast
from typing import Any, Iterable

_ALLOWED_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.And,
    ast.Or,
    ast.UnaryOp,
    ast.Not,
    ast.USub,
    ast.UAdd,
    ast.Compare,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Is,
    ast.IsNot,
    ast.BinOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Mod,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.List,
    ast.Tuple,
)

_MAX_EXPR_LEN = 500


class ExpressionError(ValueError):
    pass


def compile_expression(expression: str, columns: Iterable[str]) -> ast.Expression:
    if not expression or len(expression) > _MAX_EXPR_LEN:
        raise ExpressionError("expression is empty or too long")
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise ExpressionError(f"invalid syntax: {e.msg}") from None
    column_set = set(columns)
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ExpressionError(f"construct not allowed: {type(node).__name__}")
        if isinstance(node, ast.Name) and node.id not in column_set and node.id not in {"true", "false", "null"}:
            raise ExpressionError(f"unknown column reference {node.id!r}")
        if isinstance(node, ast.Constant) and not isinstance(node.value, (str, int, float, bool, type(None))):
            raise ExpressionError("only scalar constants are allowed")
    return tree


def evaluate_expression(tree: ast.Expression, row: dict[str, Any]) -> Any:
    return _eval(tree.body, row)


def check_expression(expression: str, columns: Iterable[str]) -> None:
    compile_expression(expression, columns)


_NULL_SENTINEL = object()


def _get(row: dict[str, Any], name: str) -> Any:
    if name == "true":
        return True
    if name == "false":
        return False
    if name == "null":
        return None
    return row.get(name)


def _eval(node: ast.AST, row: dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return _get(row, node.id)
    if isinstance(node, ast.List):
        return [_eval(e, row) for e in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_eval(e, row) for e in node.elts)
    if isinstance(node, ast.UnaryOp):
        v = _eval(node.operand, row)
        if isinstance(node.op, ast.Not):
            return not v
        if isinstance(node.op, ast.USub):
            return -v
        return +v
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            out = True
            for e in node.values:
                out = _eval(e, row)
                if not out:
                    return out
            return out
        out = False
        for e in node.values:
            out = _eval(e, row)
            if out:
                return out
        return out
    if isinstance(node, ast.BinOp):
        a, b = _eval(node.left, row), _eval(node.right, row)
        if isinstance(node.op, ast.Add):
            return a + b
        if isinstance(node.op, ast.Sub):
            return a - b
        if isinstance(node.op, ast.Mult):
            return a * b
        if isinstance(node.op, ast.Div):
            return a / b
        return a % b
    if isinstance(node, ast.Compare):
        left = _eval(node.left, row)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval(comparator, row)
            if left is None or right is None:
                return None  # SQL three-valued logic
            try:
                if isinstance(op, ast.Eq):
                    ok = left == right
                elif isinstance(op, ast.NotEq):
                    ok = left != right
                elif isinstance(op, ast.Lt):
                    ok = left < right
                elif isinstance(op, ast.LtE):
                    ok = left <= right
                elif isinstance(op, ast.Gt):
                    ok = left > right
                elif isinstance(op, ast.GtE):
                    ok = left >= right
                elif isinstance(op, ast.In):
                    ok = left in right
                elif isinstance(op, ast.NotIn):
                    ok = left not in right
                elif isinstance(op, ast.Is):
                    ok = left is right
                elif isinstance(op, ast.IsNot):
                    ok = left is not right
                else:  # pragma: no cover - parser blocks this
                    raise ExpressionError("unsupported comparison")
            except TypeError:
                # Comparing incompatible types means the row violates the
                # contract's type expectations; treat the expression as failed.
                return False
            if not ok:
                return False
            left = right
        return True
    return _NULL_SENTINEL  # pragma: no cover