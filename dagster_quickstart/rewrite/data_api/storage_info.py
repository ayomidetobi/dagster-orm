"""Physical storage location reporting for DuckLake-backed tables."""

from __future__ import annotations

import pandas as pd


def common_storage_path(data_files: pd.DataFrame) -> str | None:
    """Return the common S3/local directory prefix shared by every data file.

    e.g. a table partitioned by ticker_source/year still resolves to one
    top-level path (.../values/) rather than one entry per partition. None
    if there are no files yet (nothing written) or the column is missing.
    """

    if data_files.empty or "data_file" not in data_files.columns:
        return None

    paths = data_files["data_file"].dropna().astype(str).tolist()
    if not paths:
        return None

    prefix = paths[0]
    for path in paths[1:]:
        while not path.startswith(prefix):
            prefix = prefix[:-1]
            if not prefix:
                return None

    return prefix.rsplit("/", 1)[0] + "/" if "/" in prefix else prefix
