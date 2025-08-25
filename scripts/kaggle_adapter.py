"""
Kaggle Telco Customer Churn dataset adapter
================================================

This module converts the original Kaggle Telco Customer Churn dataset
(WA_Fn-UseC_-Telco-Customer-Churn.csv) into the schema expected by
the churn pipeline.  The Kaggle dataset contains a number of
categorical columns with capitalised names and slightly different
terminology compared to the pipeline's synthetic generator.  This
script performs the following transformations:

* Renames columns to snake_case equivalents (e.g. ``customerID`` →
  ``customer_id``, ``MonthlyCharges`` → ``monthly_charges``).
* Converts the ``SeniorCitizen`` numeric indicator (0/1) into
  ``senior_citizen`` categorical labels (``No``/``Yes``).
* Standardises boolean ``Yes``/``No`` values for partner, dependents,
  phone service, internet service and contract fields.
* Fills missing or empty total charges with zero and coerces to
  floating point.
* Drops columns not required for the baseline churn model (e.g.
  streaming services, multiple lines, payment method).  You can
  extend the mapping as needed for more advanced feature engineering.

Usage
-----

Run this script before ingestion to convert the raw Kaggle file into
``customers.csv``.  For example:

.. code-block:: bash

   python -m dm4ml_churn_pipeline.scripts.kaggle_adapter \
      --input data/raw/source_csv/20250822/WA_Fn-UseC_-Telco-Customer-Churn.csv \
      --output data/raw/source_csv/20250822/customers.csv

The output file can then be ingested using the existing ingestion
scripts without any further modification.
"""

import argparse
import os
import pandas as pd


def adapt_telco_dataset(input_path: str, output_path: str) -> None:
    """Adapt the Kaggle Telco customer churn dataset to the pipeline schema.

    Args:
        input_path: Path to the original Kaggle CSV file.
        output_path: Path to write the adapted CSV file.
    """
    # Read the raw Kaggle file.  The Kaggle dataset uses a comma
    # delimiter and may contain empty strings in the ``TotalCharges``
    # column which need to be coerced to numeric.
    df = pd.read_csv(input_path)

    # Rename columns to snake_case.  Only rename the columns used by
    # our pipeline; the rest will be dropped below.
    rename_map = {
        "customerID": "customer_id",
        "gender": "gender",
        "SeniorCitizen": "senior_citizen",
        "Partner": "partner",
        "Dependents": "dependents",
        "tenure": "tenure_months",
        "PhoneService": "phone_service",
        "InternetService": "internet_service",
        "Contract": "contract",
        "MonthlyCharges": "monthly_charges",
        "TotalCharges": "total_charges",
        "Churn": "churn",
    }
    df = df.rename(columns=rename_map)

    # Keep only the columns of interest.  Additional columns are
    # discarded to keep the dataset lean.  Should you wish to explore
    # further features (e.g. streaming services), extend this list.
    keep_cols = list(rename_map.values())
    df = df[keep_cols]

    # Convert SeniorCitizen numeric indicator to categorical labels.
    if df["senior_citizen"].dtype != object:
        df["senior_citizen"] = df["senior_citizen"].map({0: "No", 1: "Yes"})

    # Standardise boolean fields (partner, dependents, phone_service,
    # internet_service, contract) to title case for consistency.  Some
    # Kaggle values may already be "No", "Yes" or more descriptive
    # phrases (e.g. "Fiber optic", "DSL", "No internet service").  The
    # preparation module will one-hot encode these categories later.
    for col in ["partner", "dependents", "phone_service", "internet_service", "contract"]:
        df[col] = df[col].astype(str).str.strip().str.title()

    # Fix churn column to title case ("Yes"/"No").
    df["churn"] = df["churn"].astype(str).str.strip().str.title()

    # Clean numeric columns.  Some rows may have blank strings in
    # TotalCharges; coerce to NaN then fill with zero for minimal
    # baseline.  You could choose to drop such rows instead.
    df["total_charges"] = pd.to_numeric(df["total_charges"], errors="coerce")
    df["total_charges"] = df["total_charges"].fillna(0.0)
    df["monthly_charges"] = pd.to_numeric(df["monthly_charges"], errors="coerce")
    df["monthly_charges"] = df["monthly_charges"].fillna(0.0)
    df["tenure_months"] = pd.to_numeric(df["tenure_months"], errors="coerce")
    df["tenure_months"] = df["tenure_months"].fillna(0)

    # Save adapted file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Adapted dataset saved to {output_path} with {len(df)} rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Kaggle Telco dataset to pipeline schema")
    parser.add_argument("--input", required=True, help="Path to Kaggle Telco CSV file")
    parser.add_argument("--output", required=True, help="Path to write adapted CSV file")
    args = parser.parse_args()
    adapt_telco_dataset(args.input, args.output)