"""Static checks for PySpark anti-patterns that break or get expensive on Databricks serverless.

    python tools/spark_lint.py src/          # also runs in pre-commit, CI and the test suite

Serverless runs Spark Connect, so anything touching the JVM, RDDs, SparkContext or caching fails
at runtime there, while it works fine on a laptop. The rest are classic cost traps. Suppress a
finding on one line with a justification:  ``# spark-lint: ok[SPK001] bounded file listing``.
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path

RULES = {
    "SPK001": "collect()/toPandas() pulls data to the driver: bound it and justify it, or keep it in Spark",
    "SPK002": "Python UDF: row-by-row Python is slow, and native expressions are almost always possible",
    "SPK003": "RDD / SparkContext / JVM access is not available on serverless (Spark Connect)",
    "SPK004": "cache()/persist() is not supported on serverless: materialise to a Delta table instead",
    "SPK005": "inferSchema / input_file_name(): an extra full scan, unsupported in Unity Catalog. Use contracts / _metadata",
    "SPK006": "withColumn inside a loop grows the plan quadratically: use withColumns or one select",
    "SPK007": "crossJoin: cartesian product. Almost always a bug",
    "SPK008": "spark.conf.set: most settings are rejected on serverless",
    "SPK009": "to_date/to_timestamp with a format THROWS on bad input under ANSI: use try_to_timestamp",
    "SPK010": "hard-coded s3:// location: pass paths as job parameters",
    "SPK011": "repartition(1)/coalesce(1) funnels all data through one task",
}

SUPPRESS = re.compile(r"#\s*spark-lint:\s*ok\[(SPK\d{3}(?:,\s*SPK\d{3})*)\]\s*\S+")


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    code: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.code} {RULES[self.code]}"


class Visitor(ast.NodeVisitor):
    def __init__(self, path: str, lines: list[str]):
        self.path, self.lines, self.findings, self.loop_depth = path, lines, [], 0

    def add(self, node: ast.AST, code: str) -> None:
        # In a multi-line method chain the suppression sits on the line of the offending call.
        candidates = {node.lineno, getattr(node, "end_lineno", None),
                      getattr(getattr(node, "func", None), "end_lineno", None)}
        for n in sorted(c for c in candidates if c):
            m = SUPPRESS.search(self.lines[n - 1]) if n <= len(self.lines) else None
            if m and code in {c.strip() for c in m.group(1).split(",")}:
                return
        self.findings.append(Finding(self.path, node.lineno, code))

    def visit_For(self, node: ast.For) -> None:
        self.loop_depth += 1
        self.generic_visit(node)
        self.loop_depth -= 1

    visit_While = visit_For  # type: ignore[assignment]

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        for dec in node.decorator_list:
            name = dec.func if isinstance(dec, ast.Call) else dec
            if isinstance(name, (ast.Name, ast.Attribute)) and _name(name) in {"udf", "pandas_udf"}:
                self.add(node, "SPK002")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in {"rdd", "sparkContext", "_jvm", "_jdf", "_jsc"}:
            self.add(node, "SPK003")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        fn = _name(node.func)
        if fn in {"collect", "toPandas", "toLocalIterator"}:
            self.add(node, "SPK001")
        elif fn in {"udf", "pandas_udf"}:
            self.add(node, "SPK002")
        elif fn in {"cache", "persist"}:
            self.add(node, "SPK004")
        elif fn == "input_file_name" or (fn == "option" and node.args and _const(node.args[0]) == "inferSchema"):
            self.add(node, "SPK005")
        elif fn == "withColumn" and self.loop_depth:
            self.add(node, "SPK006")
        elif fn == "crossJoin":
            self.add(node, "SPK007")
        elif fn == "set" and isinstance(node.func, ast.Attribute) and _name(node.func.value) == "conf":
            self.add(node, "SPK008")
        elif fn in {"to_date", "to_timestamp"} and len(node.args) >= 2:
            self.add(node, "SPK009")
        elif fn in {"repartition", "coalesce"} and node.args and _const(node.args[0]) == 1:
            self.add(node, "SPK011")
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str) and re.search(r"s3a?://[a-z0-9][a-z0-9.-]{2,}", node.value):
            self.add(node, "SPK010")


def _name(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _const(node: ast.AST) -> object:
    return node.value if isinstance(node, ast.Constant) else None


def lint_file(path: Path) -> list[Finding]:
    text = path.read_text(encoding="utf-8")
    v = Visitor(str(path), text.splitlines())
    v.visit(ast.parse(text, filename=str(path)))
    return v.findings


def lint(paths: list[str]) -> list[Finding]:
    files = [p for root in map(Path, paths) for p in (sorted(root.rglob("*.py")) if root.is_dir() else [root])]
    return [f for p in files for f in lint_file(p)]


def main(argv: list[str]) -> int:
    findings = lint(argv or ["src"])
    for f in findings:
        print(f)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
