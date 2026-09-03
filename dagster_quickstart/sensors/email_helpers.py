"""Shared Jinja2/formatting helpers for sensors/run_notifications.py and sensors/steer_notifications.py.

Factored out so a new sensor extends the existing template system (render
one of email_templates/*.html with a context dict, send via
OutlookEmailResource) instead of introducing a second rendering approach.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from decouple import config
from jinja2 import Template

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "email_templates"


def render(template_name: str, **context: Any) -> str:
    """Render one of email_templates/*.html with the given Jinja2 context."""
    return Template((TEMPLATES_DIR / template_name).read_text()).render(**context)


def run_url(run_id: str) -> str:
    base = config("DAGSTER_WEBSERVER_URL", default="http://localhost:3000").rstrip("/")
    return f"{base}/runs/{run_id}"


def format_utc(timestamp: float | None) -> str:
    if timestamp is None:
        return "unknown"
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_duration(start: float | None, end: float | None) -> str:
    if start is None or end is None:
        return "unknown"
    total_seconds = int(end - start)
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds}s" if minutes else f"{seconds}s"


def triggered_by(tags: dict[str, str]) -> str:
    if tags.get("dagster/sensor_name"):
        return f"Sensor · {tags['dagster/sensor_name']}"
    if tags.get("dagster/schedule_name"):
        return f"Schedule · {tags['dagster/schedule_name']}"
    return "Manual"
