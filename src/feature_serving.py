import mlflow
import pandas as pd
from databricks import feature_engineering
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    OnlineTableSpec,
    OnlineTableSpecTriggeredSchedulingPolicy,
)
from pyspark.sql import SparkSession

from . import logger

logger = logger.Logger(__name__)


class FeatureServing:
    def __init__(self, config: dict, spark: SparkSession) -> None:
        self.spark: SparkSession = spark
        self.logger = logger

        # Get feature columns details
        self.num_features = config["num_features"]
        self.cat_features = config["cat_features"]
        self.target = config["target"]
        self.catalog_name = config["catalog_name"]
        self.schema_name = config["schema_name"]

        # Define table names
        self.feature_table_name = f"{self.catalog_name}.{self.schema_name}.hotel_cancels_preds"
        self.online_table_name = f"{self.catalog_name}.{self.schema_name}.hotel_cancels_preds_online"

        # Load training and test sets from Catalog
        train_set = spark.table(f"{self.catalog_name}.{self.schema_name}.train_set").toPandas()
        test_set = spark.table(f"{self.catalog_name}.{self.schema_name}.test_set").toPandas()
        self.df = pd.concat([train_set, test_set])

        # Initialize Databricks clients
        self.workspace = WorkspaceClient()
        self.fe = feature_engineering.FeatureEngineeringClient()

        # Set the MLflow registry URI
        mlflow.set_registry_uri("databricks-uc")

    def __create_feature_table(self) -> None:
        # Load the MLflow model for predictions
        model = mlflow.sklearn.load_model(f"models:/{self.catalog_name}.{self.schema_name}.hotel_cancels")

        # select features to be served, add predictions columns and ids
        preds_df: pd.DataFrame = self.df[
            ["no_of_adults", "no_of_children", "repeated_guest", "no_of_previous_cancellations"]
        ]
        preds_df["predicted_cancel"] = model.predict(self.df)
        preds_df["id"] = range(1, len(preds_df) + 1)

        preds_df = self.spark.createDataFrame(preds_df)

        # Create the feature table in Databricks
        self.fe.create_table(
            name=self.feature_table_name,
            primary_keys=["id"],
            df=preds_df,
            description="Hotel cancelations predictions feature table",
        )

        # Enable Change Data Feed
        self.spark.sql(f"""
            ALTER TABLE {self.feature_table_name}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)

    def __create_online_feature_table(self) -> None:
        """
        Creates an online feature table based on the offline feature table
        """
        spec = OnlineTableSpec(
            primary_key_columns=["id"],
            source_table_full_name=self.feature_table_name,
            run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({"triggered": "true"}),
            perform_full_copy=False,
        )

        # Create the online table in Databricks
        self.workspace.online_tables.create(name=self.online_table_name, spec=spec)
