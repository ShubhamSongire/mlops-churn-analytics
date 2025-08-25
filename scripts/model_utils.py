"""
Model training and evaluation utilities for the churn pipeline.

This module provides functions to train and evaluate machine learning
models on the engineered features.  Two baseline models are
implemented: logistic regression and random forest.  Metrics are
recorded to a CSV file and models are saved using joblib.
"""
import os
import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from datetime import datetime

from .utils import get_logger


def train_and_evaluate(features_path: str, models_dir: str, metrics_path: str) -> None:
    """Train logistic regression and random forest models and record metrics.

    Args:
        features_path: Path to CSV containing engineered features with target.
        models_dir: Directory to save trained models.
        metrics_path: Path to write metrics CSV.
    """
    os.makedirs(models_dir, exist_ok=True)
    logger = get_logger("modeling", os.path.join(os.path.dirname(features_path), "../logs"))
    df = pd.read_csv(features_path)
    # Prepare features and target
    target = df["churn"]
    # Drop non-feature columns (customer_id and churn)
    X = df.drop(columns=["customer_id", "churn"])
    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(X, target, test_size=0.3, random_state=42, stratify=target)
    # Scale numeric features for logistic regression
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    # Logistic Regression
    logreg = LogisticRegression(max_iter=1000)
    logreg.fit(X_train_scaled, y_train)
    y_pred_lr = logreg.predict(X_test_scaled)
    metrics_lr = {
        "model": "LogisticRegression",
        "accuracy": accuracy_score(y_test, y_pred_lr),
        "precision": precision_score(y_test, y_pred_lr, zero_division=0),
        "recall": recall_score(y_test, y_pred_lr, zero_division=0),
        "f1": f1_score(y_test, y_pred_lr, zero_division=0),
    }
    # Save logistic regression model and scaler
    joblib.dump(logreg, os.path.join(models_dir, "logreg_model.joblib"))
    joblib.dump(scaler, os.path.join(models_dir, "logreg_scaler.joblib"))
    # Random Forest
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)
    metrics_rf = {
        "model": "RandomForest",
        "accuracy": accuracy_score(y_test, y_pred_rf),
        "precision": precision_score(y_test, y_pred_rf, zero_division=0),
        "recall": recall_score(y_test, y_pred_rf, zero_division=0),
        "f1": f1_score(y_test, y_pred_rf, zero_division=0),
    }
    joblib.dump(rf, os.path.join(models_dir, "rf_model.joblib"))
    # Write metrics to CSV (append or create)
    metrics_df = pd.DataFrame([metrics_lr, metrics_rf])
    metrics_df["run_timestamp"] = datetime.now().isoformat()
    # If metrics file exists, append
    if os.path.exists(metrics_path):
        existing = pd.read_csv(metrics_path)
        metrics_df = pd.concat([existing, metrics_df], ignore_index=True)
    metrics_df.to_csv(metrics_path, index=False)
    logger.info(f"Models trained. Metrics saved to {metrics_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Train and evaluate churn models")
    parser.add_argument("features_path", type=str, help="Path to engineered features CSV including churn target")
    parser.add_argument("models_dir", type=str, help="Directory to save trained models")
    parser.add_argument("metrics_path", type=str, help="Path to write metrics CSV")
    args = parser.parse_args()
    train_and_evaluate(args.features_path, args.models_dir, args.metrics_path)