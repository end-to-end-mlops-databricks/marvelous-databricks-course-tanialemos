import mlflow
import pandas as pd
from databricks import feature_engineering
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    OnlineTableSpec,
    OnlineTableSpecTriggeredSchedulingPolicy,
)
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from pyspark.sql import SparkSession

from . import logger

logger = logger.Logger(__name__)


class FeatureServing:
    """
    This will not work as some packages are not supported by VSCode.
    The notebook with the same name should be used instead.
    """

    def __init__(self, config: dict, spark: SparkSession) -> None:
        self.spark: SparkSession = spark
        self.logger = logger

        # Get feature columns details
        self.num_features = config["num_features"]
        self.cat_features = config["cat_features"]
        self.catalog_name = config["catalog_name"]
        self.schema_name = config["schema_name"]
        self.lookup_features = config["lookup_features"]
        self.prediction = config["prediction"]

        # Define table names
        self.feature_table_name = f"{self.catalog_name}.{self.schema_name}.hotel_cancels_preds"
        self.online_table_name = f"{self.catalog_name}.{self.schema_name}.hotel_cancels_preds_online"

        # Load training and test sets from Catalog
        train_set = spark.table(f"{self.catalog_name}.{self.schema_name}.train_set").toPandas()
        test_set = spark.table(f"{self.catalog_name}.{self.schema_name}.test_set").toPandas()
        cols_to_drop = ["booking_status", "update_timestamp_utc"]
        train_set = train_set.drop(columns=cols_to_drop)
        test_set = test_set.drop(columns=cols_to_drop)
        self.df = pd.concat([train_set, test_set])

        # Initialize Databricks clients
        self.workspace = WorkspaceClient()
        self.fe = feature_engineering.FeatureEngineeringClient()

        # Set the MLflow registry URI
        mlflow.set_registry_uri("databricks-uc")

    def __create_feature_table(self) -> None:
        """
        Creates an offline feature table in Databricks.

        - Loads an MLflow model from Databricks UC to generate predictions.
        - Generates predictions for existing full dataset.
        - Saves selected features and predictions as a Delta table.
        - Enables Change Data Feed for the table.
        """
        logger.info("Start creating offline feature table...")
        # Load the MLflow model for predictions
        model = mlflow.sklearn.load_model(f"models:/{self.catalog_name}.{self.schema_name}.hotel_cancels_model/1")

        # select features to be served, add predictions columns and ids
        preds_df: pd.DataFrame = self.df[self.lookup_features]
        preds_df[self.prediction] = model.predict(self.df)
        preds_df["id"] = [str(i) for i in range(1, len(preds_df) + 1)]  # Ensure IDs are strings

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
        logger.info("Offline feature table created")

    def __create_online_feature_table(self) -> None:
        """
        Creates an online feature table based on the offline feature table
        """
        logger.info("Start creating online feature table...")
        spec = OnlineTableSpec(
            primary_key_columns=["id"],
            source_table_full_name=self.feature_table_name,
            run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({"triggered": "true"}),
            perform_full_copy=False,
        )

        # Create the online table in Databricks
        self.workspace.online_tables.create(name=self.online_table_name, spec=spec)
        logger.info("Online feature table created")

    def __create_serving_endpoint(self) -> None:
        """
        Creates a feature-serving endpoint for real-time predictions.
        """
        logger.info("Start creating serving endpoint...")
        # Define features to look up from the feature table
        features = [
            feature_engineering.FeatureLookup(
                table_name=self.feature_table_name,
                lookup_key="id",
                feature_names=[self.lookup_features + self.prediction],
            )
        ]

        # Create the feature spec for serving
        feature_spec_name = f"{self.catalog_name}.{self.schema_name}.return_predictions"
        self.fe.create_feature_spec(name=feature_spec_name, features=features, exclude_columns=None)

        # Create a serving endpoint for the house prices predictions
        self.workspace.serving_endpoints.create(
            name="hotel-cancels-feature-serving",
            config=EndpointCoreConfigInput(
                served_entities=[
                    ServedEntityInput(
                        entity_name=feature_spec_name,  # feature spec name defined in the previous step
                        scale_to_zero_enabled=True,
                        workload_size="Small",  # Define the workload size (Small, Medium, Large)
                    )
                ]
            ),
        )
        logger.info("Serving endpoint created")

    def deploy_feature_serving_endpoint(self) -> None:
        """
        Deploys the feature-serving endpoint.
        """
        self.__create_feature_table()
        self.__create_online_feature_table()
        self.__create_serving_endpoint()
