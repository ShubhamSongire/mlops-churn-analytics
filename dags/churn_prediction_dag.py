from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import our pipeline functions
import importlib.util
import sys
import os

# Add project root to path (one folder back from dags)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.ingestion import ingest_data
from scripts.validation import validate_data
from scripts.preparation import prepare_dataset
from scripts.transformation import transform_features
from scripts.model_utils import train_and_evaluate


# Define default arguments (without deprecated email fields)
default_args = {
    'owner': 'data_science_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG

# Create the DAG without SmtpNotifier (not available in your Airflow version)
dag = DAG(
    'churn_prediction_pipeline',
    default_args=default_args,
    description='End-to-end churn prediction pipeline',
    schedule='@daily',
    catchup=False
)

# Define the base directory
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def get_date_partition(**context):
    """Get the date partition for the current run"""
    execution_date = context['execution_date']
    return execution_date.strftime('%Y%m%d')

def run_ingest(**context):
    """Run data ingestion"""
    date_partition = get_date_partition(**context)
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    return ingest_data(raw_dir, date_partition)

def run_validate(**context):
    """Run data validation"""
    date_partition = get_date_partition(**context)
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    validated_dir = os.path.join(base_dir, 'data', 'validated')
    return validate_data(raw_dir, validated_dir, date_partition)

def run_prepare(**context):
    """Run data preparation"""
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    prepared_dir = os.path.join(base_dir, 'data', 'prepared')
    charts_dir = os.path.join(base_dir, 'docs', 'charts')
    return prepare_dataset(raw_dir, prepared_dir, charts_dir)

def run_transform(**context):
    """Run feature transformation"""
    prepared_dir = os.path.join(base_dir, 'data', 'prepared')
    transformed_dir = os.path.join(base_dir, 'data', 'transformed')
    return transform_features(prepared_dir, transformed_dir)

def run_train_evaluate(**context):
    """Run model training and evaluation"""
    features_path = os.path.join(base_dir, 'data', 'transformed', 'customer_features.csv')
    models_dir = os.path.join(base_dir, 'models')
    metrics_path = os.path.join(models_dir, 'model_metrics.csv')
    return train_and_evaluate(features_path, models_dir, metrics_path)

# Create tasks
ingest_task = PythonOperator(
    task_id='data_ingestion',
    python_callable=run_ingest,

    dag=dag
)

validate_task = PythonOperator(
    task_id='data_validation',
    python_callable=run_validate,

    dag=dag
)

prepare_task = PythonOperator(
    task_id='data_preparation',
    python_callable=run_prepare,

    dag=dag
)

transform_task = PythonOperator(
    task_id='feature_transformation',
    python_callable=run_transform,

    dag=dag
)

train_evaluate_task = PythonOperator(
    task_id='model_training_evaluation',
    python_callable=run_train_evaluate,

    dag=dag
)

# Set task dependencies
ingest_task >> validate_task >> prepare_task >> transform_task >> train_evaluate_task
