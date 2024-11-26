"""
This script handles data ingestion for a hotel booking cancellation prediction system.

Key functionality:
1. Load source dataset and retrieve recent records with recent timestamps.
2. Split new records into train and test sets.
3. Append new train and test records to existing train and test Delta Tables.
5. Set a task value indicating whether new data was processed.
"""

import argparse

import yaml
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max

from hotel_cancels import logger

log = logger.Logger(__name__)

log.info("Starting data preprocessing...")

log.info("Initial setup")

log.info("Parsing arguments...")

parser = argparse.ArgumentParser()
parser.add_argument(
    "--root_path",
    action="store",
    default=None,
    type=str,
    required=True,
)

args = parser.parse_args()
root_path = args.root_path

log.info("Loading configuration...")

with open(f"{root_path}/project_config.yml", "r") as file:
    config = yaml.safe_load(file)

pipeline_id = config["pipeline_id"]
catalog_name = config["catalog_name"]
schema_name = config["schema_name"]

log.info(f"Configuration loaded:\ncatalog_name: {catalog_name}\nschema_name: {schema_name}\npipeline_id: {pipeline_id}")

log.info("Initializing Spark session...")

spark = SparkSession.builder.getOrCreate()

log.info("Initializing workspace client...")
workspace = WorkspaceClient()

log.info("New data ingestion")

log.info("Loading source_data table...")

source_data = spark.table(f"{catalog_name}.{schema_name}.source_data")

log.info("Checking for new data based on timestamp...")

max_train_timestamp = (
    spark.table(f"{catalog_name}.{schema_name}.train_set")
    .select(spark_max("update_timestamp_utc").alias("max_update_timestamp"))
    .collect()[0]["max_update_timestamp"]
)

max_test_timestamp = (
    spark.table(f"{catalog_name}.{schema_name}.test_set")
    .select(spark_max("update_timestamp_utc").alias("max_update_timestamp"))
    .collect()[0]["max_update_timestamp"]
)

latest_timestamp = max(max_train_timestamp, max_test_timestamp)

new_data = source_data.filter(col("update_timestamp_utc") > latest_timestamp)
new_data = new_data.drop("id")

if new_data.count() > 0:
    """
    For the purposes of this course we will not use the original data preprocessing steps.
    The new data was synthetically generated based on already preprocessed data, so it can be directly added to the train and test sets.
    """

    # log.info("Start data preprocessing...")
    #
    # preprocessor = preprocessing.Preprocessor(config, spark, new_data)
    # preprocessor.preprocess_and_save_data()
    # log.info("Data preprocessing finished")
    log.info("Splitting new data into train and test sets...")

    train_size = 1 - config["test_size"]
    test_size = config["test_size"]
    new_data_train, new_data_test = new_data.randomSplit([train_size, test_size], seed=42)

    log.info("Appending new data to train and test sets...")
    new_data_train.write.mode("append").saveAsTable(f"{catalog_name}.{schema_name}.train_set")
    new_data_test.write.mode("append").saveAsTable(f"{catalog_name}.{schema_name}.test_set")

    affected_rows_train = new_data_train.count()
    affected_rows_test = new_data_test.count()

    log.info(f"{affected_rows_train} new rows were added to train set.")
    log.info(f"{affected_rows_test} new rows were added to test set.")

    refreshed = 1

else:
    log.info("No new data was found.")
    refreshed = 0
