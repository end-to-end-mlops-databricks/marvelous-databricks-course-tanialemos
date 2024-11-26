import argparse

import yaml
from databricks.sdk import WorkspaceClient
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from hotel_cancels import feature_serving, logger

log = logger.Logger(__name__)

log.info("Starting endpoint serving...")

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

with open(f"{root_path}/project_config.yml", "r") as file:
    config = yaml.safe_load(file)

log.info("Initializing Spark session...")

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

log.info("Initializing workspace client...")
workspace = WorkspaceClient()

feature_serving = feature_serving.FeatureServing(config, spark)

model_update = dbutils.jobs.taskValues.get(taskKey="evaluate_model", key="model_update")

if model_update == 1:
    model_version = dbutils.jobs.taskValues.get(taskKey="evaluate_model", key="new_model_version")
    feature_serving.overwrite_feature_table(model_version)
else:
    feature_serving.update_feature_table()
