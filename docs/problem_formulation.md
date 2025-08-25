# Problem Formulation

Customer churn occurs when existing customers stop using a company’s services or buying its products.  Addressable churn refers to situations where intervention might prevent that loss.  High churn reduces revenue and increases customer acquisition costs.  A predictive model that flags customers at risk of churn enables targeted retention efforts and improves the company’s bottom line.

## Business objectives

* **Reduce churn rate:** Proactively identify and retain customers likely to churn.
* **Maximise customer lifetime value:** Focus on retaining high‑value customers and increasing engagement.
* **Automate the data pipeline:** Build an end‑to‑end system that continuously ingests, validates, prepares and serves data for machine learning.

## Data sources

| Source | Description | Key attributes |
| --- | --- | --- |
| `customers.csv` | Demographic and account data per customer | customer_id, gender, senior_citizen, partner, dependents, tenure_months, monthly_charges, total_charges, contract, internet_service, phone_service, churn |
| `transactions.csv` | Transaction history for each customer | transaction_id, customer_id, transaction_date, amount |
| `web_logs.jsonl` | Web events captured from the customer portal | customer_id, timestamp, event_type |

Additional sources could include support tickets, CRM interactions or external demographics.

## Expected pipeline outputs

* **Cleaned datasets for EDA** – validated and cleaned versions of each raw source suitable for analysis.
* **Transformed features for modelling** – aggregated and engineered features such as total spend, transaction recency and one‑hot encoded categorical variables, stored in both CSV and a relational database.
* **Deployable churn model** – trained models (logistic regression and random forest) saved for inference and accompanied by metrics.

## Evaluation metrics

Model performance will be assessed on a hold‑out test set using:

* **Accuracy** – proportion of correct predictions.
* **Precision** – proportion of predicted churn cases that were correct.
* **Recall** – proportion of actual churners that were identified.
* **F1 score** – harmonic mean of precision and recall.

These metrics provide a balanced view of performance on an imbalanced churn dataset and support comparison between algorithms.