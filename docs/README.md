# DM4ML Churn Pipeline

This project implements an end‑to‑end data management pipeline for a
customer churn prediction use case.  It follows the assignment
specification by covering the entire workflow from problem
formulation through ingestion, validation, preparation, feature
engineering, feature storage, versioning, modelling and orchestration.

## Structure

```
dm4ml_churn_pipeline/
  data/
    raw/                   # raw source and ingested data
    prepared/              # cleaned datasets for EDA and modelling
    validated/             # validation reports per date partition
    transformed/           # engineered feature tables
    logs/                  # log files from various scripts
    versions.json          # version history of data artefacts
  database/               # SQLite warehouse (customer_features)
  feature_store/          # SQLite feature store
  models/                 # trained models and metrics
  docs/
    raw_storage.md         # documentation of raw storage design
    README.md              # this file
    charts/                # EDA charts
    screenshots/           # screenshots of raw data and previews
  scripts/                # Python modules for each stage
  notebooks/             # Jupyter notebooks for each stage
```

## Pipeline overview

1. **Problem formulation** – Define business problem, objectives, data sources and expected outputs; captured in notebook `01_problem_formulation.ipynb`.
2. **Data generation** – Synthetic Telco‑like customer data, transactions and web logs are generated (see `scripts/data_generator.py`).  In a real scenario this would be replaced with the actual Kaggle Telco data.
3. **Ingestion** – Raw data (CSV and JSONL) is ingested into the raw layer with timestamped filenames.  See `scripts/ingestion.py` and notebook `02_ingestion.ipynb`.
4. **Raw storage** – The `docs/raw_storage.md` file describes the date and source‑partitioned folder structure used to store raw data.
5. **Validation** – Automated checks for missing values, invalid ranges and duplicates produce a quality report stored in `data/validated/<date>/`.  See `scripts/validation.py` and notebook `04_validation.ipynb`.
6. **Preparation & EDA** – Clean the customers data, perform basic exploratory analysis, save EDA charts and a cleaned dataset.  See `scripts/preparation.py` and notebook `05_preparation_eda.ipynb`.
7. **Transformation & Storage** – Aggregate transactions and derive additional features (e.g., total spend, recency), one‑hot encode categorical variables and save results to a CSV and a SQLite database.  See `scripts/transformation.py` and notebook `06_transformation_and_storage.ipynb`.
8. **Feature store** – A minimal feature store built on SQLite records feature definitions and numeric feature values by version.  See `scripts/feature_store.py` and notebook `07_feature_store.ipynb`.
9. **Data versioning** – MD5 hashes of raw and transformed artefacts are recorded in `data/versions.json` to track changes over time.  See `scripts/versioning.py` and notebook `08_data_versioning.ipynb`.
10. **Model building** – Train logistic regression and random forest models on the engineered features, evaluate them and persist both the models and metrics.  See `scripts/model_utils.py` and notebook `09_model_building.ipynb`.
11. **Orchestration** – A simple orchestrator coordinates the entire pipeline end‑to‑end.  Running `python -m dm4ml_churn_pipeline.scripts.orchestrator dm4ml_churn_pipeline <date>` will execute every stage in sequence.  See `scripts/orchestrator.py` and notebook `10_orchestration.ipynb`.

## Getting started

1. Clone or download this repository and ensure all Python dependencies (pandas, numpy, scikit‑learn, joblib) are installed.
2. (Optional) Generate synthetic data for your chosen date partition using the provided generator: 
   `python -m dm4ml_churn_pipeline.scripts.data_generator --output-root dm4ml_churn_pipeline/data/raw/source_csv --date YYYYMMDD --n-customers 5000`.

3. **To use the real Kaggle Telco dataset**, first download the file
   ``WA_Fn-UseC_-Telco-Customer-Churn.csv`` from Kaggle (or another
   source that mirrors the Kaggle dataset).  Then run the adapter
   script to convert it into the schema expected by the pipeline:

   ```bash
   python -m dm4ml_churn_pipeline.scripts.kaggle_adapter \
       --input data/raw/source_csv/YYYYMMDD/WA_Fn-UseC_-Telco-Customer-Churn.csv \
       --output data/raw/source_csv/YYYYMMDD/customers.csv
   ```

   After adaptation you can proceed with ingestion as normal.

4. Run the pipeline end‑to‑end: `python -m dm4ml_churn_pipeline.scripts.orchestrator dm4ml_churn_pipeline YYYYMMDD`.
5. Review the generated artefacts under the `data/`, `database/`, `feature_store/` and `models/` folders.
6. Open the notebooks in order to explore each stage interactively.

## Versioning strategy

A simple versioning approach is implemented via the `scripts/versioning.py` module.  Each time the pipeline runs it calculates MD5 hashes for ingested files and the engineered features CSV and appends them to `data/versions.json` along with a timestamp.  This provides a lightweight record of when datasets were created and facilitates reproducibility.

## Notes

* The synthetic data generator approximates the structure of the Kaggle Telco churn dataset.  To use the real dataset, replace the generated files in `data/raw/source_csv/<date>/` with the actual Kaggle CSVs before running the ingestion script.
* The pipeline uses local directories and SQLite databases for simplicity.  In a production environment these could be replaced with cloud storage (S3/GCS), distributed processing (Spark), a data warehouse (BigQuery/Redshift) and an orchestrator like Apache Airflow.
