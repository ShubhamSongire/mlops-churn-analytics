"""
Orchestration script for the churn pipeline.

This script coordinates each stage of the pipeline, from synthetic
data generation through ingestion, validation, preparation,
transformation, feature store population, versioning, and model
training.  By executing this script you can reproduce an end‑to‑end
workflow in a single run.

Note: In a real world deployment this orchestration would likely be
managed by a tool such as Apache Airflow, Prefect or Dagster.  Here we
implement a simple linear orchestration in code to satisfy the
assignment requirements.
"""
import os
from datetime import datetime

from .data_generator import generate_customers, generate_transactions, generate_web_logs
from .ingestion import ingest_csv, ingest_api
from .validation import generate_quality_report
from .preparation import prepare_dataset
from .transformation import transform_and_store
from .feature_store import register_features, write_feature_values
from .versioning import record_versions
from .model_utils import train_and_evaluate


def run_pipeline(base_dir: str, date_partition: str, version: str = "v1") -> None:
    """Run the complete churn pipeline.

    Args:
        base_dir: Root directory of the pipeline (containing data, scripts, etc.).
        date_partition: Date string for source partition (YYYYMMDD).
        version: Feature/version identifier.
    """
    # Paths
    source_csv_dir = os.path.join(base_dir, "data", "raw", "source_csv", date_partition)
    source_api_dir = os.path.join(base_dir, "data", "raw", "source_api", date_partition)
    raw_root = os.path.join(base_dir, "data", "raw")
    prepared_dir = os.path.join(base_dir, "data", "prepared")
    charts_dir = os.path.join(base_dir, "docs", "charts")
    validated_dir = os.path.join(base_dir, "data", "validated", date_partition)
    transformed_dir = os.path.join(base_dir, "data", "transformed")
    db_path = os.path.join(base_dir, "database", "churn_dw.sqlite")
    feature_db = os.path.join(base_dir, "feature_store", "feature_store.sqlite")
    models_dir = os.path.join(base_dir, "models")
    metrics_path = os.path.join(models_dir, "model_metrics.csv")
    versions_path = os.path.join(base_dir, "data", "versions.json")

    # Step 1: Data generation (only if source files missing)
    if not os.path.exists(os.path.join(source_csv_dir, "customers.csv")):
        os.makedirs(source_csv_dir, exist_ok=True)
        os.makedirs(source_api_dir, exist_ok=True)
        customers = generate_customers()
        transactions = generate_transactions(customers)
        web_logs = generate_web_logs(customers)
        customers.to_csv(os.path.join(source_csv_dir, "customers.csv"), index=False)
        transactions.to_csv(os.path.join(source_csv_dir, "transactions.csv"), index=False)
        with open(os.path.join(source_api_dir, "web_logs.jsonl"), "w") as f:
            import json
            for event in web_logs:
                json.dump(event, f)
                f.write("\n")

    # Step 2: Ingest raw data
    ingest_csv(source_csv_dir, raw_root)
    ingest_api(os.path.join(source_api_dir, "web_logs.jsonl"), raw_root)

    # Step 3: Validate
    generate_quality_report(raw_root, validated_dir)

    # Step 4: Prepare
    prepare_dataset(raw_root, prepared_dir, charts_dir)

    # Step 5: Transform & store
    features_csv = transform_and_store(raw_root, prepared_dir, transformed_dir, db_path)

    # Step 6: Feature store population
    import pandas as pd
    features_df = pd.read_csv(features_csv)
    register_features(features_df, feature_db, version=version)
    write_feature_values(features_df, feature_db, version=version)

    # Step 7: Versioning raw and transformed data
    files_to_version = []
    # Latest ingested files
    for fname in os.listdir(raw_root):
        if fname.startswith("ingested"):
            files_to_version.append(os.path.join(raw_root, fname))
    files_to_version.append(features_csv)
    record_versions(files_to_version, versions_path)

    # Step 8: Model training
    train_and_evaluate(features_csv, models_dir, metrics_path)

    print("Pipeline executed successfully.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the entire churn pipeline")
    parser.add_argument("base_dir", type=str, help="Root directory of the pipeline (e.g., dm4ml_churn_pipeline)")
    parser.add_argument("date_partition", type=str, default=datetime.now().strftime("%Y%m%d"), nargs='?', help="Date partition (YYYYMMDD)")
    parser.add_argument("--version", type=str, default="v1", help="Version tag for features")
    args = parser.parse_args()
    run_pipeline(args.base_dir, args.date_partition, version=args.version)