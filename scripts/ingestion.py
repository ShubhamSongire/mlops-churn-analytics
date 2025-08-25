def ingest_data(raw_root: str, date_partition: str) -> None:
    """Ingest all CSV and API data for a given date partition into the raw layer."""
    source_csv_dir = os.path.join(raw_root, 'source_csv', date_partition)
    source_api_file = os.path.join(raw_root, 'source_api', date_partition, 'web_logs.jsonl')
    ingest_csv(source_csv_dir, raw_root)
    ingest_api(source_api_file, raw_root)
"""
Data ingestion module for the churn pipeline.

This script defines functions to ingest structured CSV data and
semi‑structured JSONL API data into the raw data layer.  Each
ingestion run produces a timestamped copy of the source files in the
`data/raw` directory and logs details about the ingest such as row
counts and any errors encountered.
"""
import os
import shutil
from datetime import datetime
import pandas as pd

from .utils import get_logger


def ingest_csv(source_dir: str, raw_root: str) -> None:
    """Ingest all CSV files from a source directory into the raw layer.

    Each CSV file is read with pandas to verify that it can be parsed.
    A timestamped copy of the original file is then written to
    ``raw_root``.  Row counts and any exceptions are logged.

    Args:
        source_dir: Directory containing source CSV files.
        raw_root: Root directory where ingested files will be stored.
    """
    logger = get_logger("ingest_csv", os.path.join(raw_root, "../logs"))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for filename in os.listdir(source_dir):
        if not filename.lower().endswith(".csv"):
            continue
        src_path = os.path.join(source_dir, filename)
        dest_filename = f"ingested_{timestamp}_{filename}"
        dest_path = os.path.join(raw_root, dest_filename)
        try:
            df = pd.read_csv(src_path)
            row_count = len(df)
            df.to_csv(dest_path, index=False)
            logger.info(f"Ingested {filename} with {row_count} rows to {dest_filename}")
        except Exception as e:
            logger.exception(f"Failed to ingest {filename}: {e}")


def ingest_api(jsonl_path: str, raw_root: str) -> None:
    """Ingest a JSONL web log file into the raw layer.

    The file is copied to ``raw_root`` with a timestamped prefix.  The
    number of events (lines) is counted and logged.  Any exceptions are
    logged.

    Args:
        jsonl_path: Path to the source JSONL file.
        raw_root: Root directory where ingested files will be stored.
    """
    logger = get_logger("ingest_api", os.path.join(raw_root, "../logs"))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.basename(jsonl_path)
    dest_filename = f"ingested_{timestamp}_{filename}"
    dest_path = os.path.join(raw_root, dest_filename)
    try:
        # Count lines to estimate number of events
        with open(jsonl_path, "r") as f:
            count = sum(1 for _ in f)
        shutil.copy2(jsonl_path, dest_path)
        logger.info(f"Ingested {filename} with {count} events to {dest_filename}")
    except Exception as e:
        logger.exception(f"Failed to ingest API file {filename}: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest CSV and API data into raw layer")
    parser.add_argument("source_csv_dir", type=str, help="Directory containing source CSV files")
    parser.add_argument("source_api_file", type=str, help="Path to JSONL API file")
    parser.add_argument("raw_root", type=str, help="Destination raw root directory (e.g. data/raw)")
    args = parser.parse_args()
    ingest_csv(args.source_csv_dir, args.raw_root)
    ingest_api(args.source_api_file, args.raw_root)