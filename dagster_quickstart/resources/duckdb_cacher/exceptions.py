"""Exceptions raised by the DuckDB/DuckLake connection layer.

DuckLakeConfigError is defined in rewrite.data_api.errors (part of that
package's RewriteError hierarchy, so callers can catch it alongside every
other domain error there) and re-exported here so this package has its own
self-contained exceptions module, rather than reaching into rewrite/ from
several different files in this folder.
"""

from __future__ import annotations

from rewrite.data_api.errors import DuckLakeConfigError

__all__ = ["DuckLakeConfigError"]
