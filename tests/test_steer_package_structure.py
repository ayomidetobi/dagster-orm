"""Enforces steer/'s internal package layering (see steer/__init__.py's module docstring for
the target layout):

    constants, errors  ->  source/  ->  analytics/  ->  config, orm, model  ->  run

constants.py/errors.py are leaves; source/ and analytics/ may import only those leaves (plus
stdlib/third-party, plus their own sibling submodules) from the rest of steer/; analytics/
additionally may never import rewrite.data_api at all (no I/O in the math layer);
config.py/orm.py/model.py sit above both and may import from source/analytics/leaves freely,
but never from run.py.

Only MODULE-LEVEL imports count against the layering (imports that run automatically just by
importing the file). Imports inside a function/method body are this repo's sanctioned escape
hatch for an otherwise-upward reference (see steer/orm.py's and steer/config.py's module
docstrings for why) and are deliberately exempt here -- they carry zero import-time cost, by
construction, since they only ever run when the specific function is called. `if
TYPE_CHECKING:` blocks are exempt for the same reason (compile-time only, never executed).
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Set

STEER_DIR = Path(__file__).resolve().parents[1] / "dagster_quickstart" / "steer"

_ALLOWED_BELOW_SOURCE_AND_ANALYTICS = {"constants", "errors"}


def _is_type_checking_guard(test: ast.expr) -> bool:
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    if isinstance(test, ast.Attribute):
        return test.attr == "TYPE_CHECKING"
    return False


def _module_level_steer_submodule_imports(path: Path) -> Set[str]:
    """Every top-level `dagster_quickstart.steer.<X>` this file imports at module level.

    Descends into every node except function/method bodies (skipped entirely -- see module
    docstring) and `if TYPE_CHECKING:` blocks (skipped entirely -- never executed at runtime).
    """
    tree = ast.parse(path.read_text())
    found: Set[str] = set()
    prefix = "dagster_quickstart.steer."

    def visit(node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue  # function-body imports are the sanctioned escape hatch
            if isinstance(child, ast.If) and _is_type_checking_guard(child.test):
                continue  # never executes at runtime
            if isinstance(child, ast.ImportFrom) and child.module and child.module.startswith(prefix):
                found.add(child.module[len(prefix):].split(".")[0])
            elif isinstance(child, ast.Import):
                for alias in child.names:
                    if alias.name.startswith(prefix):
                        found.add(alias.name[len(prefix):].split(".")[0])
            visit(child)

    visit(tree)
    return found


def test_source_only_imports_leaves_of_the_rest_of_steer_at_module_level():
    for path in (STEER_DIR / "source").rglob("*.py"):
        imports = _module_level_steer_submodule_imports(path)
        disallowed = imports - _ALLOWED_BELOW_SOURCE_AND_ANALYTICS - {"source"}
        assert not disallowed, f"{path} imports steer.{disallowed} -- source/ may only import constants/errors"


def test_analytics_only_imports_leaves_of_the_rest_of_steer_at_module_level():
    for path in (STEER_DIR / "analytics").rglob("*.py"):
        imports = _module_level_steer_submodule_imports(path)
        disallowed = imports - _ALLOWED_BELOW_SOURCE_AND_ANALYTICS - {"analytics"}
        assert not disallowed, f"{path} imports steer.{disallowed} -- analytics/ may only import constants/errors"


def test_analytics_has_no_io():
    for path in (STEER_DIR / "analytics").rglob("*.py"):
        assert "rewrite.data_api" not in path.read_text(), path


def test_config_orm_model_never_import_run():
    for name in ("config.py", "orm.py", "model.py"):
        path = STEER_DIR / name
        disallowed = _module_level_steer_submodule_imports(path) & {"run"}
        assert not disallowed, f"{path} imports steer.run"
