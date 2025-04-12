# Historical Data Transformer CLI Usage

This document describes how to use the `historical_data_transformer.py` script from the command line.

## Purpose

The script fetches historical price and NAV data for a specific date from a designated source table (e.g., `TestHistoricalData` in `HistoricalPriceDB`). It then transforms this data into two separate formats:

1.  **Price Data:** Mimics the structure expected from the `RealTimeData` table.
2.  **NAV Data:** Mimics the structure expected from the `RealtickNAVs` table.

Finally, it saves these transformed datasets as CSV files into a designated output directory, ready to be potentially used by other scripts like `DataConductor.py` for backfilling purposes.

## Usage

```bash
python historical_data_transformer.py --date <YYYY-MM-DD> --price-output <path/to/output.csv> --nav-output <path/to/output.csv>

python historical_data_transformer.py --date 2025-04-10 --price-output ./output/price_file_2025_04_10.csv --nav-output ./output/nav_file_2025_04_10.csv
```

## Arguments

*   `--date`
    *   **Required:** Yes
    *   **Format:** `YYYY-MM-DD`
    *   **Description:** Specifies the target date for which to fetch and transform data.

*   `--price-output`
    *   **Required:** Yes
    *   **Format:** File path, typically within the `./output/` directory (e.g., `./output/historical_price_20240115.csv`)
    *   **Description:** The full path, including the filename, where the transformed price data CSV file should be saved.

*   `--nav-output`
    *   **Required:** Yes
    *   **Format:** File path, typically within the `./output/` directory (e.g., `./output/historical_nav_20240115.csv`)
    *   **Description:** The full path, including the filename, where the transformed NAV data CSV file should be saved.

## Examples

1.  **Fetch data for January 15, 2024, and save to the `./output/` directory:**

    ```bash
    python historical_data_transformer.py --date 2024-01-15 --price-output ./output/historical_price_20240115.csv --nav-output ./output/historical_nav_20240115.csv
    ```

2.  **Fetch data for December 1, 2023, and save to the `./output/` directory with different names:**

    ```bash
    python historical_data_transformer.py --date 2023-12-01 --price-output ./output/price_data_20231201.csv --nav-output ./output/nav_data_20231201.csv
    ``` 