def validate_data(raw_root: str, validated_dir: str, date_partition: str) -> None:
    """Wrapper for Airflow DAG: calls generate_quality_report for the given raw and validated directories."""
    return generate_quality_report(raw_root, validated_dir)
"""
Data validation module for the churn pipeline.

This script provides functions to perform automated checks on the
ingested raw data.  The validation functions read the latest ingested
files and produce a CSV report summarising data quality issues.  It
also logs the results for auditing.
"""
import json
import os
import re
import pandas as pd
from datetime import datetime
from typing import Dict, List

from .utils import get_logger


def _find_latest_ingested_files(raw_root: str) -> Dict[str, str]:
    """Identify the most recent ingested CSV and JSONL files in raw_root.

    Args:
        raw_root: Directory containing ingested files.

    Returns:
        Mapping from logical name (customers, transactions, web_logs) to
        file path.
    """
    files = os.listdir(raw_root)
    pattern = re.compile(r"ingested_\d{8}_\d{6}_(.+)\.(csv|jsonl)")
    latest = {}
    for f in files:
        m = pattern.match(f)
        if not m:
            continue
        name = m.group(1).split(".")[0]
        full_path = os.path.join(raw_root, f)
        # Determine if this file is the latest by timestamp prefix
        # timestamp portion is ingested_YYYYMMDD_HHMMSS
        ts_str = f.split("_")[1] + f.split("_")[2]
        ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
        if name not in latest or ts > latest[name]["ts"]:
            latest[name] = {"path": full_path, "ts": ts}
    return {k: v["path"] for k, v in latest.items()}


def validate_customers(customers: pd.DataFrame) -> Dict[str, int]:
    """Perform quality checks on customers data.

    Checks include missing values, invalid ranges, and duplicates.

    Returns a dictionary of issue counts.
    """
    issues = {
        "missing_customer_id": customers["customer_id"].isnull().sum(),
        "duplicate_customer_id": customers["customer_id"].duplicated().sum(),
        "negative_tenure_months": (customers["tenure_months"] < 0).sum(),
        "missing_monthly_charges": customers["monthly_charges"].isnull().sum(),
        "missing_total_charges": customers["total_charges"].isnull().sum(),
    }
    return issues


def validate_transactions(transactions: pd.DataFrame) -> Dict[str, int]:
    """Perform quality checks on transactions data.

    Checks include missing values, invalid dates, negative amounts and duplicates.
    """
    issues = {
        "missing_transaction_id": transactions["transaction_id"].isnull().sum(),
        "duplicate_transaction_id": transactions["transaction_id"].duplicated().sum(),
        "missing_customer_id": transactions["customer_id"].isnull().sum(),
        "negative_amount": (transactions["amount"] < 0).sum(),
    }
    # Validate date format
    invalid_dates = 0
    for date_str in transactions["transaction_date"]:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            invalid_dates += 1
    issues["invalid_dates"] = invalid_dates
    return issues


def validate_web_logs(log_path: str) -> Dict[str, int]:
    """Perform basic checks on web log JSONL file.

    Currently checks for lines that are not valid JSON.
    """
    invalid_json = 0
    with open(log_path, "r") as f:
        for line in f:
            try:
                json.loads(line)
            except json.JSONDecodeError:
                invalid_json += 1
    return {"invalid_json": invalid_json}


def generate_quality_report(raw_root: str, validated_dir: str) -> None:
    """Generate a data quality report for the latest ingested files.

    The report is saved as a CSV in the validated directory.  It
    contains issue counts for each dataset.  A corresponding log
    message summarises the results.

    Args:
        raw_root: Directory where ingested files reside.
        validated_dir: Directory to write validation report.
    """
    os.makedirs(validated_dir, exist_ok=True)
    logger = get_logger("validation", os.path.join(raw_root, "../logs"))
    latest_files = _find_latest_ingested_files(raw_root)
    report_records: List[Dict[str, object]] = []
    for name, path in latest_files.items():
        if name == "customers":
            df = pd.read_csv(path)
            issues = validate_customers(df)
        elif name == "transactions":
            df = pd.read_csv(path)
            issues = validate_transactions(df)
        elif name == "web_logs":
            issues = validate_web_logs(path)
        else:
            continue
        for issue_name, count in issues.items():
            report_records.append({
                "dataset": name,
                "issue": issue_name,
                "count": count,
            })
        logger.info(f"Validated {name} - issues: {issues}")
    # Save report
    report_df = pd.DataFrame(report_records)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(validated_dir, f"quality_report_{timestamp}.csv")
    report_df.to_csv(report_path, index=False)
    logger.info(f"Quality report generated at {report_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Validate ingested datasets and generate quality report")
    parser.add_argument("raw_root", type=str, help="Directory containing ingested raw files")
    parser.add_argument("validated_dir", type=str, help="Output directory for validation reports")
    args = parser.parse_args()
    generate_quality_report(args.raw_root, args.validated_dir)