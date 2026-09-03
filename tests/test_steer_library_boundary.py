"""Enforces the steer/ vs assets/steer/ boundary: steer/ (the library) must never import Dagster.

assets/steer/ (the Dagster layer) owns orchestration only -- partitions, resources, Output/
AssetCheckResult/MetadataValue, context.log. steer/ owns every domain decision and must be
callable from a plain script or test, with no Dagster runtime involved.
"""

from __future__ import annotations

import re
from pathlib import Path

STEER_DIR = Path(__file__).resolve().parents[1] / "dagster_quickstart" / "steer"

#: Matches a real Dagster-framework import ("import dagster", "from dagster import ...",
#: "from dagster.something import ...") but NOT this project's own dagster_quickstart package.
#: A plain substring check ("import dagster" in source) has a false-positive on exactly that:
#: `from dagster_quickstart.steer.config import StrategyConfig` contains the literal substring
#: "from dagster" (dagster_quickstart starts with "dagster"), so a naive check would flag every
#: normal intra-project import in this file's own package. The word boundary after "dagster"
#: (whitespace, ".", or end of line) is what distinguishes the framework from this project.
_DAGSTER_IMPORT_PATTERN = re.compile(r"^\s*(import|from)\s+dagster(\s|\.|$)", re.MULTILINE)


def test_steer_library_never_imports_dagster():
    offenders = []
    for path in STEER_DIR.rglob("*.py"):
        source = path.read_text()
        if _DAGSTER_IMPORT_PATTERN.search(source):
            offenders.append(path)
    assert offenders == []


def test_the_dagster_import_pattern_does_not_false_positive_on_this_projects_own_package():
    """Regression test for the pattern itself: dagster_quickstart.steer.* imports (this
    project's own package, which legitimately starts with "dagster") must NOT match."""
    assert not _DAGSTER_IMPORT_PATTERN.search(
        "from dagster_quickstart.steer.config import StrategyConfig\n"
    )
    assert not _DAGSTER_IMPORT_PATTERN.search("import dagster_quickstart.steer.discovery\n")


def test_the_dagster_import_pattern_does_catch_real_dagster_imports():
    assert _DAGSTER_IMPORT_PATTERN.search("import dagster\n")
    assert _DAGSTER_IMPORT_PATTERN.search("from dagster import asset\n")
    assert _DAGSTER_IMPORT_PATTERN.search("from dagster.core import something\n")
