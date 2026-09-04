"""Enforces dagster_quickstart.availability's package boundary: it must never reference `steer`.

availability/ defines the generic SHAPE of an availability check; steer/ supplies STEER's
values (role filters, required roles, variant names) via AvailabilitySpec. Importing anything
from dagster_quickstart.steer here would mean the package has drifted back into defining
STEER's own drivers/variants, which defeats the point of the extraction (see
availability/__init__.py's module docstring).
"""

from __future__ import annotations

from pathlib import Path

AVAILABILITY_DIR = Path(__file__).resolve().parents[1] / "dagster_quickstart" / "availability"


def test_availability_never_imports_steer():
    for path in AVAILABILITY_DIR.rglob("*.py"):
        assert "steer" not in path.read_text(), path
