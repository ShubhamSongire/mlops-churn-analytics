"""
Data preparation module for the churn pipeline.

This script cleans and preprocesses the raw customer data and performs
exploratory data analysis (EDA).  The output is a prepared dataset
ready for feature engineering and model training, along with basic
visualisations summarising the data distributions.
"""
import os
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for script execution
import matplotlib.pyplot as plt

from .utils import get_logger
from .validation import _find_latest_ingested_files


NUMERIC_COLUMNS = ["tenure_months", "monthly_charges", "total_charges"]
CATEGORICAL_COLUMNS = ["gender", "senior_citizen", "partner", "dependents",
                       "contract", "internet_service", "phone_service"]
TARGET_COLUMN = "churn"


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the customers dataset.

    - Remove duplicates.
    - Handle missing numeric values via median imputation.
    - Convert categorical types.
    - Drop rows with missing target.

    Args:
        df: Raw customer DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    # Drop duplicates based on customer_id
    df = df.drop_duplicates(subset=["customer_id"])
    # Handle missing target
    df = df.dropna(subset=[TARGET_COLUMN])
    # Impute numeric missing values with median
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
    # Convert senior_citizen to categorical label if not already
    if df["senior_citizen"].dtype != object:
        df["senior_citizen"] = df["senior_citizen"].map({0: "No", 1: "Yes"})
    # Ensure churn is binary 1/0
    df[TARGET_COLUMN] = df[TARGET_COLUMN].map({"Yes": 1, "No": 0})
    return df


def prepare_dataset(raw_root: str, prepared_dir: str, charts_dir: str) -> str:
    """Prepare data for analysis and modelling.

    Args:
        raw_root: Directory containing ingested raw files.
        prepared_dir: Directory to write the prepared dataset.
        charts_dir: Directory to save EDA charts.

    Returns:
        Path to the prepared CSV file.
    """
    os.makedirs(prepared_dir, exist_ok=True)
    os.makedirs(charts_dir, exist_ok=True)
    logger = get_logger("preparation", os.path.join(raw_root, "../logs"))

    # Identify latest customers file
    latest_files = _find_latest_ingested_files(raw_root)
    customers_path = latest_files.get("customers")
    if not customers_path:
        logger.error("No ingested customers file found for preparation.")
        raise FileNotFoundError("Customers data not found")
    customers = pd.read_csv(customers_path)
    cleaned = clean_customers(customers)

    # Generate EDA charts
    for col in NUMERIC_COLUMNS:
        if col in cleaned.columns:
            fig, ax = plt.subplots()
            ax.hist(cleaned[col], bins=30)
            ax.set_title(f"Histogram of {col}")
            ax.set_xlabel(col)
            ax.set_ylabel("Frequency")
            fig.savefig(os.path.join(charts_dir, f"hist_{col}.png"))
            plt.close(fig)
            # Box plot
            fig, ax = plt.subplots()
            ax.boxplot(cleaned[col])
            ax.set_title(f"Boxplot of {col}")
            ax.set_ylabel(col)
            fig.savefig(os.path.join(charts_dir, f"box_{col}.png"))
            plt.close(fig)
    for col in CATEGORICAL_COLUMNS:
        if col in cleaned.columns:
            fig, ax = plt.subplots()
            cleaned[col].value_counts().plot(kind='bar', ax=ax)
            ax.set_title(f"Distribution of {col}")
            ax.set_xlabel(col)
            ax.set_ylabel("Count")
            fig.savefig(os.path.join(charts_dir, f"bar_{col}.png"))
            plt.close(fig)
    # Save cleaned dataset
    prepared_path = os.path.join(prepared_dir, "customers_prepared.csv")
    cleaned.to_csv(prepared_path, index=False)
    logger.info(f"Prepared dataset saved to {prepared_path} with {len(cleaned)} rows.")
    return prepared_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Prepare customer data for EDA and modelling")
    parser.add_argument("raw_root", type=str, help="Directory containing ingested raw files")
    parser.add_argument("prepared_dir", type=str, help="Output directory for prepared data")
    parser.add_argument("charts_dir", type=str, help="Directory to store EDA charts")
    args = parser.parse_args()
    prepare_dataset(args.raw_root, args.prepared_dir, args.charts_dir)