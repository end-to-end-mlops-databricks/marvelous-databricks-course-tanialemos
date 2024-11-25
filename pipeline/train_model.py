"""
This script trains a sklearn logistic regression model to predict hotel booking cancellations.

Key functionality:
- Loads training and test data from Databricks Delta tables
- Trains a logistic regression model
- Tracks the experiment using MLflow
- Logs model metrics, parameters and artifacts
- Outputs model URI for downstream tasks
"""

import argparse

import yaml
from databricks.sdk import WorkspaceClient
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from src import logger, model_training

log = logger.Logger(__name__)

log.info("Starting model training...")

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
parser.add_argument(
    "--git_sha",
    action="store",
    default=None,
    type=str,
    required=True,
)
parser.add_argument(
    "--job_run_id",
    action="store",
    default=None,
    type=str,
    required=True,
)

args = parser.parse_args()
root_path = args.root_path
git_sha = args.git_sha
job_run_id = args.job_run_id

log.info("Loading configuration...")

with open(f"{root_path}/project_config.yml", "r") as file:
    config = yaml.safe_load(file)

log.info("Initializing Spark session...")

spark = SparkSession.builder.getOrCreate()

log.info("Initializing workspace client...")
workspace = WorkspaceClient()

log.info("Start model training and logging...")

model_training = model_training.ModelTraining(config, spark, args)
model_uri = model_training.train_and_log_model()

log.info("Model training and logging finished")

dbutils = DBUtils(spark)
dbutils.jobs.taskValues.set(key="new_model_uri", value=model_uri)
