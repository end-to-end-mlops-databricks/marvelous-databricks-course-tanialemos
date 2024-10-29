from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler

from . import logger

log = logger.Logger(__name__)


def load_data(spark, filepath):
    log.info("Data loading...")

    try:
        df = spark.read.csv(filepath, header=True)
    except Exception as e:
        log.error(e)

    log.info(f"Databricks df shape: {(df.count(), len(df.columns))}")

    return df


def clean_and_preprocess_data(df, config):
    log.info("Data cleaning and preprocessing...")

    log.info("Print schema")
    df.printSchema()

    log.info("Check unique Booking Status values")
    df.select("booking_status").distinct().show()

    df = df.dropDuplicates()
    log.info(f"DF shape after dropDuplicates: {(df.count(), len(df.columns))}")

    # Select specified features and target
    df_features = df.select(config["num_features"] + config["cat_features"]).toPandas()
    target = df.select(config["target"]).toPandas()

    # Create preprocessing steps
    numeric_transformer = Pipeline(steps=[("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())])

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    # Combine preprocessing steps
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, config["num_features"]),
            ("cat", categorical_transformer, config["cat_features"]),
        ]
    )

    log.info("Transform pipeline...")
    X = preprocessor.fit_transform(df_features)

    # Preprocess target variable
    target_encoder = LabelEncoder()
    y = target_encoder.fit_transform(target)

    log.info(f"X-array shape: {X.shape}")
    log.info("Finished data cleaning and preprocessing")

    return X, y


def split_data(X, y, test_size=0.2, random_state=42):
    log.info("Split data...")
    return train_test_split(X, y, test_size=test_size, random_state=random_state, stratify=y)


def load_and_preprocess(spark, filepath, config):
    df = load_data(spark, filepath)
    X, y = clean_and_preprocess_data(df, config)
    X_train, X_test, y_train, y_test = split_data(X, y)

    return X_train, X_test, y_train, y_test
