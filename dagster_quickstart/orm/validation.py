"""Validation functions for metadata against lookup tables."""

from typing import Any, Dict, List

import pandas as pd

from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.exceptions import (
    SeriesNotFoundError,
)
from dagster_quickstart.orm.schema import (
    LOOKUP_TABLE_PROCESSING_ORDER,
    MetadataColumns,
)


class MetadataValidator:
    """Validator for metadata against lookup tables."""

    def __init__(self, metadata_repository: MetadataRepository):
        """Initialize MetadataValidator with metadata repository.

        Args:
            metadata_repository: MetadataRepository instance for loading lookup tables
        """
        self._metadata_repository = metadata_repository
        self._lookup_table_cache: pd.DataFrame | None = None

    def _load_lookup_table(self) -> pd.DataFrame:
        """Load lookup table from S3 (cached).

        Returns:
            DataFrame with lookup table data
        """
        if self._lookup_table_cache is None:
            lookup_uri = self._metadata_repository._s3_adapter.get_lookup_uri()
            query_builder, param_values = self._metadata_repository._build_filtered_query(None)
            adapted_sql, builder_params = (
                self._metadata_repository._parquet_adapter.adapt_query_builder_for_parquet(
                    query_builder, lookup_uri
                )
            )
            all_params = param_values + builder_params
            if all_params:
                self._lookup_table_cache = self._metadata_repository._repository.execute_raw_sql(
                    adapted_sql, all_params
                )
            else:
                self._lookup_table_cache = self._metadata_repository._repository.execute_raw_sql(
                    adapted_sql
                )
        return self._lookup_table_cache

    def _get_lookup_values(self, lookup_type: str) -> List[str]:
        """Get valid lookup values for a given lookup type.

        Args:
            lookup_type: Type of lookup (e.g., 'asset_class', 'field_type')

        Returns:
            List of valid lookup values (from both code and name columns)
        """
        lookup_df = self._load_lookup_table()

        filtered = lookup_df[lookup_df["lookup_type"] == lookup_type]

        valid_values = set()

        if "code" in filtered.columns:
            code_values = filtered["code"].dropna().unique().tolist()
            valid_values.update(str(v).strip() for v in code_values if v)

        if "name" in filtered.columns:
            name_values = filtered["name"].dropna().unique().tolist()
            valid_values.update(str(v).strip() for v in name_values if v)

        return sorted(list(valid_values))

    def validate_metadata_row(
        self,
        metadata_row: Dict[str, Any],
    ) -> List[str]:
        """Validate a single metadata row against lookup tables.

        Args:
            metadata_row: Dictionary representing a metadata row

        Returns:
            List of validation error messages (empty if valid)
        """
        errors: List[str] = []
        series_code = metadata_row.get(MetadataColumns.SERIES_CODE, "")

        for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
            if lookup_type not in metadata_row:
                continue

            lookup_value = metadata_row[lookup_type]
            if lookup_value is None or str(lookup_value).strip() == "":
                continue

            lookup_value_str = str(lookup_value).strip()
            valid_values = self._get_lookup_values(lookup_type)

            if lookup_value_str not in valid_values:
                errors.append(
                    f"Series '{series_code}' has invalid {lookup_type} '{lookup_value_str}'. "
                    f"Valid values: {valid_values[:5]}..."
                    if len(valid_values) > 5
                    else f"Valid values: {valid_values}"
                )

        return errors

    def validate_metadata_dataframe(
        self,
        metadata_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Validate metadata DataFrame against lookup tables.

        Args:
            metadata_df: DataFrame with metadata rows

        Returns:
            DataFrame with only valid rows (invalid rows are filtered out)

        Raises:
            SeriesNotFoundError: If all rows are invalid
        """
        if metadata_df.empty:
            raise SeriesNotFoundError("Metadata DataFrame is empty")

        valid_rows = []
        all_errors: List[str] = []

        for _, row in metadata_df.iterrows():
            row_dict = row.to_dict()
            errors = self.validate_metadata_row(row_dict)

            if not errors:
                valid_rows.append(row_dict)
            else:
                all_errors.extend(errors)

        if not valid_rows:
            error_msg = "All metadata rows failed validation:\n" + "\n".join(all_errors[:10])
            if len(all_errors) > 10:
                error_msg += f"\n... and {len(all_errors) - 10} more errors"
            raise SeriesNotFoundError(error_msg)

        if all_errors:
            warning_msg = f"Found {len(all_errors)} validation errors:\n" + "\n".join(
                all_errors[:5]
            )
            if len(all_errors) > 5:
                warning_msg += f"\n... and {len(all_errors) - 5} more errors"

        return pd.DataFrame(valid_rows)
