# API Documentation

## Data Ingestion API

### CSV Ingestion
```python
from scripts.ingestion import ingest_csv

def ingest_csv(source_dir: str, raw_root: str) -> None:
    """
    Ingest CSV files from source directory with versioning.

    Args:
        source_dir (str): Source directory path containing CSV files
        raw_root (str): Root directory for storing raw data

    Returns:
        None

    Raises:
        FileNotFoundError: If source directory doesn't exist
        ValidationError: If data fails validation checks
    """
```

### API Data Ingestion
```python
from scripts.ingestion import ingest_api

def ingest_api(api_file: str, raw_root: str) -> None:
    """
    Ingest API data from JSON Lines file.

    Args:
        api_file (str): Path to API data file
        raw_root (str): Root directory for storing raw data

    Returns:
        None

    Raises:
        FileNotFoundError: If API file doesn't exist
        JSONDecodeError: If JSON data is invalid
    """
```

## Data Validation API

### Schema Validation
```python
from scripts.validation import validate_schema

def validate_schema(data: pd.DataFrame, schema: dict) -> List[str]:
    """
    Validate DataFrame against provided schema.

    Args:
        data (pd.DataFrame): Input DataFrame
        schema (dict): Schema definition

    Returns:
        List[str]: List of validation issues

    Raises:
        SchemaValidationError: If schema is invalid
    """
```

### Quality Checks
```python
from scripts.validation import check_data_quality

def check_data_quality(data: pd.DataFrame) -> dict:
    """
    Perform data quality checks.

    Args:
        data (pd.DataFrame): Input DataFrame

    Returns:
        dict: Quality check results

    Raises:
        DataQualityError: If critical quality checks fail
    """
```

## Feature Engineering API

### Feature Transformation
```python
from scripts.transformation import transform_features

def transform_features(data: pd.DataFrame) -> pd.DataFrame:
    """
    Apply feature transformations.

    Args:
        data (pd.DataFrame): Input DataFrame

    Returns:
        pd.DataFrame: Transformed DataFrame

    Raises:
        TransformationError: If feature transformation fails
    """
```

### Feature Store Operations
```python
from scripts.feature_store import FeatureStore

class FeatureStore:
    def __init__(self, db_path: str):
        """
        Initialize feature store.

        Args:
            db_path (str): Path to SQLite database
        """

    def save_features(self, features: pd.DataFrame, version: str):
        """
        Save features to store.

        Args:
            features (pd.DataFrame): Feature DataFrame
            version (str): Feature version
        """

    def load_features(self, version: str) -> pd.DataFrame:
        """
        Load features from store.

        Args:
            version (str): Feature version

        Returns:
            pd.DataFrame: Feature DataFrame
        """
```

## Model API

### Model Training
```python
from scripts.model_utils import train_model

def train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    model_type: str,
    params: dict
) -> Any:
    """
    Train model with given parameters.

    Args:
        X_train (pd.DataFrame): Training features
        y_train (pd.Series): Training labels
        model_type (str): Type of model
        params (dict): Model parameters

    Returns:
        Any: Trained model object

    Raises:
        ModelTrainingError: If training fails
    """
```

### Model Evaluation
```python
from scripts.model_utils import evaluate_model

def evaluate_model(
    model: Any,
    X_test: pd.DataFrame,
    y_test: pd.Series
) -> dict:
    """
    Evaluate model performance.

    Args:
        model: Trained model object
        X_test (pd.DataFrame): Test features
        y_test (pd.Series): Test labels

    Returns:
        dict: Performance metrics
    """
```

## Utility Functions

### Data Versioning
```python
from scripts.utils import version_data

def version_data(data: pd.DataFrame, version: str) -> None:
    """
    Version data with metadata.

    Args:
        data (pd.DataFrame): Data to version
        version (str): Version identifier
    """
```

### Logging
```python
from scripts.utils import setup_logging

def setup_logging(log_path: str) -> None:
    """
    Setup logging configuration.

    Args:
        log_path (str): Path to log file
    """
```
