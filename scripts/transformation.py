def transform_features(*args, **kwargs):
    """Wrapper for Airflow DAG compatibility. Calls transform_and_store with same arguments."""
    return transform_and_store(*args, **kwargs)
"""
Transformation and feature engineering module for the churn pipeline.

This script reads the prepared customer data and combines it with
transaction history to derive aggregated features.  The resulting
feature table is saved both as a CSV in the transformed layer and to a
SQLite database acting as a simple data warehouse.
"""
import os
from datetime import datetime
import pandas as pd
import numpy as np
import sqlite3

from .utils import get_logger
from .validation import _find_latest_ingested_files


def engineer_features(prepared_df: pd.DataFrame, transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Create aggregated features combining customer and transaction data.

    Args:
        prepared_df: Cleaned customer DataFrame with numeric and encoded target.
        transactions_df: Transaction history DataFrame.

    Returns:
        DataFrame with engineered features.
    """
    # Aggregate transactions per customer
    if not transactions_df.empty:
        # Ensure transaction_date is datetime
        transactions_df["transaction_date"] = pd.to_datetime(transactions_df["transaction_date"], errors='coerce')
        agg = transactions_df.groupby("customer_id").agg({
            "amount": ["count", "sum", "mean"],
            "transaction_date": "max",
        })
        agg.columns = ["num_transactions", "total_spend", "avg_transaction_amount", "last_transaction_date"]
        agg = agg.reset_index()
    else:
        # If no transactions, create empty DataFrame
        agg = pd.DataFrame(columns=["customer_id", "num_transactions", "total_spend", "avg_transaction_amount", "last_transaction_date"])
    # Merge with prepared data
    df = prepared_df.merge(agg, on="customer_id", how="left")
    df["num_transactions"] = df["num_transactions"].fillna(0)
    df["total_spend"] = df["total_spend"].fillna(0)
    df["avg_transaction_amount"] = df["avg_transaction_amount"].fillna(0)
    # Compute recency (days since last transaction) – if never transacted, set to max value
    today = datetime.now()
    df["last_transaction_date"] = pd.to_datetime(df["last_transaction_date"])
    df["recency_days"] = (today - df["last_transaction_date"]).dt.days
    df["recency_days"] = df["recency_days"].fillna(df["recency_days"].max())
    # Drop raw last_transaction_date column – recency_days captures the temporal information in numeric form
    df = df.drop(columns=["last_transaction_date"])
    # Derive tenure in years
    df["tenure_years"] = df["tenure_months"] / 12.0
    # Normalise numeric features using min-max scaling
    for col in ["tenure_months", "monthly_charges", "total_charges", "total_spend", "avg_transaction_amount", "recency_days", "tenure_years"]:
        min_val = df[col].min()
        max_val = df[col].max()
        if max_val > min_val:
            df[f"{col}_scaled"] = (df[col] - min_val) / (max_val - min_val)
        else:
            df[f"{col}_scaled"] = 0.0

    # One-hot encode categorical columns to numeric form for modelling
    cat_cols = [
        "gender",
        "senior_citizen",
        "partner",
        "dependents",
        "contract",
        "internet_service",
        "phone_service",
    ]
    # Perform one-hot encoding, dropping the first level to avoid multicollinearity
    df = pd.get_dummies(df, columns=cat_cols, prefix=cat_cols, drop_first=True)
    return df


def transform_and_store(raw_root: str, prepared_dir: str, transformed_dir: str, db_path: str) -> str:
    """Execute feature engineering and save results.

    Args:
        raw_root: Directory containing ingested raw files.
        prepared_dir: Directory with prepared customer data.
        transformed_dir: Directory to save transformed CSV.
        db_path: Path to SQLite database.

    Returns:
        Path to the transformed CSV file.
    """
    os.makedirs(transformed_dir, exist_ok=True)
    logger = get_logger("transformation", os.path.join(raw_root, "../logs"))
    # Load prepared customers
    prepared_path = os.path.join(prepared_dir, "customers_prepared.csv")
    customers = pd.read_csv(prepared_path)
    # Load latest transactions
    latest_files = _find_latest_ingested_files(raw_root)
    transactions_path = latest_files.get("transactions")
    transactions = pd.read_csv(transactions_path) if transactions_path else pd.DataFrame(columns=["transaction_id", "customer_id", "transaction_date", "amount"])
    # Engineer features
    features_df = engineer_features(customers, transactions)
    # Save to CSV
    transformed_path = os.path.join(transformed_dir, "customer_features.csv")
    features_df.to_csv(transformed_path, index=False)
    logger.info(f"Transformed features saved to {transformed_path} with {len(features_df)} rows.")
    # Save to SQLite database
    conn = sqlite3.connect(db_path)
    table_name = "customer_features"
    features_df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    logger.info(f"Features written to SQLite database table {table_name}")
    return transformed_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Transform prepared data into engineered features and store them")
    parser.add_argument("raw_root", type=str, help="Directory containing ingested raw files")
    parser.add_argument("prepared_dir", type=str, help="Directory with prepared data")
    parser.add_argument("transformed_dir", type=str, help="Output directory for transformed features")
    parser.add_argument("db_path", type=str, help="Path to SQLite database file")
    args = parser.parse_args()
    transform_and_store(args.raw_root, args.prepared_dir, args.transformed_dir, args.db_path)