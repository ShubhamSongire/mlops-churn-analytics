"""
Minimal feature store implementation using SQLite.

The feature store consists of two tables:

* **feature_definitions** – stores metadata about each feature, including
  name, description, data type, source and version.
* **feature_values** – stores feature values per customer and version.

The `register_features` function writes the definitions for all columns
of a provided DataFrame.  The `write_feature_values` function writes
customer feature values for a given version.  The `get_feature_values`
function retrieves features on demand.
"""
import os
import sqlite3
from typing import Iterable, List, Dict, Any
import pandas as pd


def init_feature_store(db_path: str) -> None:
    """Create feature store tables if they do not exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_definitions (
            feature_name TEXT PRIMARY KEY,
            description TEXT,
            dtype TEXT,
            source TEXT,
            version TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_values (
            customer_id TEXT,
            feature_name TEXT,
            value REAL,
            version TEXT,
            PRIMARY KEY (customer_id, feature_name, version)
        )
        """
    )
    conn.commit()
    conn.close()


def register_features(df: pd.DataFrame, db_path: str, version: str, source: str = "transformation") -> None:
    """Register feature definitions from a DataFrame's columns.

    Args:
        df: DataFrame containing engineered features.
        db_path: Path to feature store SQLite database.
        version: Version identifier for this set of features.
        source: Source stage for these features.
    """
    init_feature_store(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for col in df.columns:
        if col == "customer_id" or col == "churn":
            continue  # not registering id or target as features
        dtype = str(df[col].dtype)
        description = f"Auto‑generated feature for column {col}"
        cur.execute(
            "INSERT OR REPLACE INTO feature_definitions (feature_name, description, dtype, source, version) VALUES (?, ?, ?, ?, ?)",
            (col, description, dtype, source, version)
        )
    conn.commit()
    conn.close()


def write_feature_values(df: pd.DataFrame, db_path: str, version: str) -> None:
    """Write feature values to the feature store.

    Args:
        df: DataFrame containing engineered features (including customer_id and features).
        db_path: Path to feature store SQLite database.
        version: Version identifier.
    """
    init_feature_store(db_path)
    conn = sqlite3.connect(db_path)
    records = []
    for _, row in df.iterrows():
        cid = row["customer_id"]
        for col in df.columns:
            if col == "customer_id" or col == "churn":
                continue
            value = row[col]
            # Only store numeric values; skip non-numeric types
            try:
                numeric_value = float(value)
                records.append((cid, col, numeric_value, version))
            except (ValueError, TypeError):
                # Skip storing categorical/string features in this minimal feature store
                continue
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO feature_values (customer_id, feature_name, value, version) VALUES (?, ?, ?, ?)",
        records
    )
    conn.commit()
    conn.close()


def get_feature_values(customer_ids: Iterable[str], feature_names: Iterable[str], db_path: str, version: str) -> pd.DataFrame:
    """Retrieve feature values for given customers and features.

    Args:
        customer_ids: Iterable of customer identifiers.
        feature_names: Iterable of feature names.
        db_path: Path to feature store.
        version: Version identifier to query.

    Returns:
        DataFrame with customer_id as index and feature names as columns.
    """
    conn = sqlite3.connect(db_path)
    # Prepare placeholders
    customer_ids = list(customer_ids)
    feature_names = list(feature_names)
    placeholders_c = ",".join(["?"] * len(customer_ids))
    placeholders_f = ",".join(["?"] * len(feature_names))
    query = f"""
        SELECT customer_id, feature_name, value
        FROM feature_values
        WHERE version = ? AND customer_id IN ({placeholders_c}) AND feature_name IN ({placeholders_f})
    """
    params = [version] + customer_ids + feature_names
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    if df.empty:
        return pd.DataFrame(columns=feature_names, index=customer_ids)
    pivot = df.pivot(index="customer_id", columns="feature_name", values="value")
    # Ensure all requested columns exist
    for fn in feature_names:
        if fn not in pivot.columns:
            pivot[fn] = np.nan
    # Ensure order of customers
    pivot = pivot.reindex(customer_ids)
    return pivot.reset_index()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Populate feature store from engineered features and query it")
    parser.add_argument("features_csv", type=str, help="Path to engineered features CSV (includes customer_id)")
    parser.add_argument("db_path", type=str, help="Path to feature store SQLite database")
    parser.add_argument("version", type=str, help="Version identifier")
    parser.add_argument("--query", nargs='*', default=[], help="If provided, query feature values for the given customer_ids and features (format: cid1,cid2:feature1,feature2)")
    args = parser.parse_args()
    df = pd.read_csv(args.features_csv)
    register_features(df, args.db_path, version=args.version)
    write_feature_values(df, args.db_path, version=args.version)
    if args.query:
        # Example query input: C000001,C000010 tenure_months,total_spend
        cid_str, feature_str = args.query
        cids = cid_str.split(",")
        features = feature_str.split(",")
        result = get_feature_values(cids, features, args.db_path, version=args.version)
        print(result)