# Databricks notebook source

# Either install wheel on notebook and run the following 2 cells. Otherwise install wheel on cluster.
# MAGIC %pip install mlops_with_databricks-0.0.1-py3-none-any.whl

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

"""
!! This notebook has to be run in the UI as some packages are not supported by VSCode !!

Create feature table in unity catalog, it will be a delta table
Create online table which uses the feature delta table created in the previous step
Create a feature spec. When you create a feature spec,
you specify the source Delta table.
This allows the feature spec to be used in both offline and online scenarios.
For online lookups, the serving endpoint automatically uses the online table to perform low-latency feature lookups.
The source Delta table and the online table must use the same primary key.

"""

import time

import mlflow
import pandas as pd
import requests
import yaml
from databricks import feature_engineering
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    OnlineTable,
    OnlineTableSpec,
    OnlineTableSpecTriggeredSchedulingPolicy,
)
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# Initialize Databricks clients
workspace = WorkspaceClient()
fe = feature_engineering.FeatureEngineeringClient()

# Set the MLflow registry URI
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load config, train and test tables

# COMMAND ----------

# Load config
with open("../project_config.yml", "r") as file:
    config = yaml.safe_load(file)

# Get feature columns details
num_features = config["num_features"]
cat_features = config["cat_features"]
catalog_name = config["catalog_name"]
schema_name = config["schema_name"]
lookup_features = config["lookup_features"]
prediction = config["prediction"]

# Define table names
feature_table_name = f"{catalog_name}.{schema_name}.hotel_cancels_preds"
online_table_name = f"{catalog_name}.{schema_name}.hotel_cancels_preds_online"

# Load training and test sets from Catalog
train_set = spark.table(f"{catalog_name}.{schema_name}.train_set").toPandas()
test_set = spark.table(f"{catalog_name}.{schema_name}.test_set").toPandas()
cols_to_drop = ["booking_status", "update_timestamp_utc"]
train_set = train_set.drop(columns=cols_to_drop)
test_set = test_set.drop(columns=cols_to_drop)
df = pd.concat([train_set, test_set])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load a registered model

# COMMAND ----------

# Load the MLflow model for predictions
model = mlflow.sklearn.load_model(f"models:/{catalog_name}.{schema_name}.hotel_cancels_model/1")

# COMMAND ----------

# select features to be served, add predictions columns and ids
preds_df: pd.DataFrame = df[lookup_features]
preds_df[prediction] = model.predict(df)
preds_df["id"] = [str(i) for i in range(1, len(preds_df) + 1)]  # Ensure IDs are string
preds_df = spark.createDataFrame(preds_df)

# Create the feature table in Databricks
fe.create_table(
    name=feature_table_name,
    primary_keys=["id"],
    df=preds_df,
    description="Hotel cancelations predictions feature table",
)

# Enable Change Data Feed
spark.sql(f"""
    ALTER TABLE {feature_table_name}
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# Create the online table using feature table

spec = OnlineTableSpec(
    primary_key_columns=["id"],
    source_table_full_name=feature_table_name,
    run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({"triggered": "true"}),
    perform_full_copy=False,
)

online_table = OnlineTable(name=online_table_name, spec=spec)

# Create the online table in Databricks
online_table_pipeline = workspace.online_tables.create(table=online_table)

# COMMAND ----------
# Create feture look up and feature spec table feature table

# Define features to look up from the feature table
features = [
    feature_engineering.FeatureLookup(
        table_name=feature_table_name,
        lookup_key="id",
        feature_names=[lookup_features + [prediction]],
    )
]
# Create the feature spec for serving
feature_spec_name = f"{catalog_name}.{schema_name}.return_predictions"

fe.create_feature_spec(name=feature_spec_name, features=features, exclude_columns=None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Feature Serving Endpoint

# COMMAND ----------
# Create endpoing using feature spec

# Create a serving endpoint for the house prices predictions
# note: it takes some time to create the endpoint
workspace.serving_endpoints.create(
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call The Endpoint

# COMMAND ----------


# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

# COMMAND ----------

id_list = preds_df["id"]

# COMMAND ----------

# COMMAND ----------

start_time = time.time()
serving_endpoint = f"https://{host}/serving-endpoints/hotel-cancels-feature-serving/invocations"
response = requests.post(
    f"{serving_endpoint}",
    headers={"Authorization": f"Bearer {token}"},
    json={"dataframe_records": [{"id": "55"}]},
)

end_time = time.time()
execution_time = end_time - start_time

print("Response status:", response.status_code)
print("Reponse text:", response.text)
print("Execution time:", execution_time, "seconds")
