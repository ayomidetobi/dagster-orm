# CSV Data Files for Dagster Quickstart

This directory contains CSV files for populating lookup tables and meta series in the financial platform.

## File Structure

### Allowed Names CSV File
A single CSV file that defines the allowed/valid names for all lookup table types (like an enum). Contains:
- `lookup_table_type` - The type of lookup table (asset_class, product_type, etc.)
- `name` - The allowed name for that lookup table type

File: `allowed_names.csv`

### Lookup Tables CSV File
A single CSV file in wide format where each lookup table type is a column. Contains:
- `id` - Row identifier (optional, for reference)
- `asset_class` - Asset class names
- `product_type` - Product type names
- `sub_asset_class` - Sub-asset class names
- `data_type` - Data type names
- `structure_type` - Structure type names
- `market_segment` - Market segment names
- `field_type` - Field type names
- `ticker_source` - Ticker source names

Each column contains the values for that lookup table type. Empty cells are allowed. The loader extracts unique non-empty values from each column when loading a specific lookup table type.

File: `lookup_tables.csv`

### Meta Series CSV File
- `meta_series.csv` - Meta series data with all required fields

## Loading Order

**IMPORTANT**: You must load lookup tables BEFORE loading meta series, because meta series references lookup table IDs.

### Step 1: Load Lookup Tables
Load each lookup table type in this order:

1. `asset_class` - Base asset classes
2. `product_type` - Product types
3. `field_type` - Field types
4. `ticker_source` - Ticker sources
5. `data_type` - Data types
6. `structure_type` - Structure types
7. `market_segment` - Market segments
8. `sub_asset_class` - Sub-asset classes (requires asset_class_id, so load after asset_class)

### Step 2: Get Lookup Table IDs
After loading lookup tables, query the database to get the IDs for each lookup table entry. You'll need these IDs for the meta_series.csv file.

### Step 3: Update Meta Series CSV
Update `meta_series.csv` with the correct lookup table IDs:
- `field_type_id` - ID from field_type lookup table
- `asset_class_id` - ID from asset_class lookup table
- `sub_asset_class_id` - ID from sub_asset_class lookup table (optional)
- `product_type_id` - ID from product_type lookup table
- `data_type_id` - ID from data_type lookup table
- `structure_type_id` - ID from structure_type lookup table (optional)
- `market_segment_id` - ID from market_segment lookup table (optional)
- `ticker_source_id` - ID from ticker_source lookup table

### Step 4: Load Meta Series
Load the meta series from the updated CSV file.

## Usage Example

### Loading a Lookup Table

```python
# In Dagster UI or code, configure the asset:
# The same CSV file is used for all lookup table types
# The loader extracts values from the column matching the lookup_table_type
config = {
    "csv_path": "data/lookup_tables.csv",
    "allowed_names_csv_path": "data/allowed_names.csv",
    "lookup_table_type": "asset_class"  # This will read from the 'asset_class' column
}
```

The loader will:
1. Read the CSV file
2. Extract the column matching the `lookup_table_type` (e.g., "asset_class" column for "asset_class" type)
3. Get all unique, non-empty values from that column
4. Validate each value against the allowed names CSV
5. Insert validated values into the database

### Loading Meta Series

```python
config = {
    "csv_path": "data/meta_series.csv"
}
```

## CSV Column Requirements

### Meta Series CSV Required Columns:
- `series_name` - Name of the series
- `series_code` - Unique code for the series
- `data_source` - Either "RAW", "DERIVED", or "NONE"
- `field_type_id` - ID from field_type lookup
- `asset_class_id` - ID from asset_class lookup
- `product_type_id` - ID from product_type lookup
- `data_type_id` - ID from data_type lookup
- `ticker_source_id` - ID from ticker_source lookup
- `ticker` - Ticker symbol

### Meta Series CSV Optional Columns:
- `sub_asset_class_id` - ID from sub_asset_class lookup
- `structure_type_id` - ID from structure_type lookup
- `market_segment_id` - ID from market_segment lookup
- `calculation_formula` - Formula for derived series
- `description` - Description of the series

## Notes

- All names in lookup table CSV files are validated against the corresponding allowed names CSV
- If a name is not in the allowed list, the loading will fail with a clear error message
- Lookup table IDs are auto-generated, so you'll need to query the database after loading to get the actual IDs
- The meta_series.csv file provided has placeholder IDs (1, 2, etc.) - you must update these with actual IDs after loading lookup tables

