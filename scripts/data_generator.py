"""
Generate synthetic customer churn data and related transaction and web log files.

This script generates three datasets:

1. **customers.csv** – A synthetic representation of the Telco Customer Churn dataset.  Each row
   corresponds to a unique customer with demographic and service attributes along with a churn flag.
2. **transactions.csv** – Simulated transactional history for each customer.  Each transaction
   contains a transaction identifier, a customer identifier, a transaction date and an amount.
3. **web_logs.jsonl** – Simulated web log events for each customer.  Each event is a JSON
   object containing a timestamp, customer identifier and event type.

The generated files are written to a date‑partitioned folder under the provided output root.

Usage::

    python data_generator.py --output-root /path/to/data/raw/source_csv --date YYYYMMDD

By default the script writes to the current date partition inside the `data/raw` directory.

"""
import argparse
import json
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def generate_customers(n_customers: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic customer churn dataset.

    Args:
        n_customers: Number of customers to generate.
        seed: Random seed for reproducibility.

    Returns:
        DataFrame with synthetic customer attributes.
    """
    rng = np.random.default_rng(seed)
    customer_ids = [f"C{str(i).zfill(6)}" for i in range(1, n_customers + 1)]
    genders = rng.choice(["Male", "Female"], size=n_customers)
    senior_citizens = rng.choice([0, 1], size=n_customers, p=[0.85, 0.15])
    partners = rng.choice(["Yes", "No"], size=n_customers, p=[0.45, 0.55])
    dependents = rng.choice(["Yes", "No"], size=n_customers, p=[0.3, 0.7])
    tenure_months = rng.integers(0, 73, size=n_customers)
    monthly_charges = rng.uniform(20.0, 120.0, size=n_customers).round(2)
    total_charges = (monthly_charges * tenure_months + rng.normal(0, 50, size=n_customers)).round(2)
    # Assign contract types with typical distribution
    contract = rng.choice(["Month-to-month", "One year", "Two year"], size=n_customers, p=[0.65, 0.25, 0.10])
    internet_service = rng.choice(["DSL", "Fiber optic", "No"], size=n_customers, p=[0.4, 0.45, 0.15])
    phone_service = rng.choice(["Yes", "No"], size=n_customers, p=[0.9, 0.1])
    # Churn probability influenced by contract type and tenure (shorter tenure and month-to-month contracts churn more)
    base_churn_prob = np.where(contract == "Month-to-month", 0.25, np.where(contract == "One year", 0.1, 0.05))
    churn_flags = rng.random(n_customers) < base_churn_prob
    churn = np.where(churn_flags, "Yes", "No")
    # Compile DataFrame
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "gender": genders,
        "senior_citizen": senior_citizens,
        "partner": partners,
        "dependents": dependents,
        "tenure_months": tenure_months,
        "monthly_charges": monthly_charges,
        "total_charges": total_charges,
        "contract": contract,
        "internet_service": internet_service,
        "phone_service": phone_service,
        "churn": churn,
    })
    return df


def generate_transactions(customers: pd.DataFrame, max_transactions_per_customer: int = 10, seed: int = 123) -> pd.DataFrame:
    """Generate synthetic transaction history for each customer.

    Args:
        customers: DataFrame of customers.
        max_transactions_per_customer: Maximum number of transactions per customer.
        seed: Random seed.

    Returns:
        DataFrame of transactions.
    """
    rng = np.random.default_rng(seed)
    records = []
    transaction_id = 1
    for customer_id in customers["customer_id"]:
        # Determine number of transactions for this customer
        n_tx = rng.integers(0, max_transactions_per_customer + 1)
        for _ in range(n_tx):
            amount = max(0, rng.normal(loc=50, scale=30))  # ensure positive values
            # Random date within last 2 years
            days_ago = rng.integers(0, 365 * 2)
            tx_date = datetime.now() - timedelta(days=int(days_ago))
            records.append({
                "transaction_id": f"T{transaction_id:08d}",
                "customer_id": customer_id,
                "transaction_date": tx_date.strftime("%Y-%m-%d"),
                "amount": round(amount, 2),
            })
            transaction_id += 1
    return pd.DataFrame(records)


def generate_web_logs(customers: pd.DataFrame, events_per_customer: int = 5, seed: int = 456) -> list:
    """Generate synthetic web log events for each customer.

    Args:
        customers: DataFrame of customers.
        events_per_customer: Approximate number of events per customer.
        seed: Random seed.

    Returns:
        List of JSON serializable log events.
    """
    rng = np.random.default_rng(seed)
    event_types = ["login", "page_view", "add_to_cart", "support_request"]
    events = []
    for customer_id in customers["customer_id"]:
        n_events = rng.integers(max(1, events_per_customer - 2), events_per_customer + 3)
        for _ in range(n_events):
            minutes_ago = rng.integers(0, 60 * 24 * 60)  # within ~60 days
            ts = datetime.now() - timedelta(minutes=int(minutes_ago))
            event_type = rng.choice(event_types, p=[0.4, 0.4, 0.15, 0.05])
            events.append({
                "customer_id": customer_id,
                "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "event_type": event_type,
            })
    return events


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic customer churn data")
    parser.add_argument(
        "--output-root",
        type=str,
        default="../data/raw/source_csv",
        help="Root directory to store generated data."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y%m%d"),
        help="Date partition to write (YYYYMMDD)."
    )
    parser.add_argument(
        "--n-customers",
        type=int,
        default=5000,
        help="Number of customers to generate."
    )
    args = parser.parse_args()

    # Construct directories
    csv_dir = os.path.join(args.output_root, args.date)
    api_dir = csv_dir.replace("source_csv", "source_api")
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(api_dir, exist_ok=True)

    # Generate data
    customers = generate_customers(n_customers=args.n_customers)
    transactions = generate_transactions(customers)
    web_logs = generate_web_logs(customers)

    # Write data
    customers.to_csv(os.path.join(csv_dir, "customers.csv"), index=False)
    transactions.to_csv(os.path.join(csv_dir, "transactions.csv"), index=False)
    # Save web logs as JSONL
    web_log_path = os.path.join(api_dir, "web_logs.jsonl")
    with open(web_log_path, "w") as f:
        for event in web_logs:
            json.dump(event, f)
            f.write("\n")

    print(f"Generated {len(customers)} customers, {len(transactions)} transactions and {len(web_logs)} web log events.")


if __name__ == "__main__":
    main()