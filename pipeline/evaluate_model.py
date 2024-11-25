"""
This script evaluates and compares a new hotel booking cancellation prediction model against the currently deployed model.
Key functionality:
- Loads test data
- Generates predictions using both new and existing models
- Calculates and compares performance metrics (Accuracy and recall)
- Registers the new model if it performs better
- Sets task values for downstream pipeline steps

"""

import argparse

import mlflow
import yaml
from databricks.sdk import WorkspaceClient
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score

from src import logger

log = logger.Logger(__name__)

log.info("Starting model evaluation...")

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
    "--new_model_uri",
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

parser.add_argument(
    "--git_sha",
    action="store",
    default=None,
    type=str,
    required=True,
)

args = parser.parse_args()
root_path = args.root_path
new_model_uri = args.new_model_uri
job_run_id = args.job_run_id
git_sha = args.git_sha

log.info("Loading configuration...")

with open(f"{root_path}/project_config.yml", "r") as file:
    config = yaml.safe_load(file)

log.info("Initializing Spark session...")

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

log.info("Initializing workspace client...")
workspace = WorkspaceClient()

log.info("Setting MLflow uris...")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_tracking_uri("databricks")

# Extract configuration details
num_features = config["num_features"]
cat_features = config["cat_features"]
target = config["target"]
catalog_name = config["catalog_name"]
schema_name = config["schema_name"]
serving_endpoint_name = config["serving_endpoint_name"]

log.info("Generate predictions for new and existing model")

log.info("Loading existing model...")

# Load currently served model based on its uri
serving_endpoint = workspace.serving_endpoints.get(serving_endpoint_name)
model_name = serving_endpoint.config.served_models[0].model_name
model_version = serving_endpoint.config.served_models[0].model_version
existing_model_uri = f"models:/{model_name}/{model_version}"
existing_model: LogisticRegression = mlflow.sklearn.load_model(existing_model_uri)

log.info("Loading new model...")
new_model: LogisticRegression = mlflow.sklearn.load_model(new_model_uri)

log.info("Loading test set...")

# Load test set
test_set = spark.table(f"{catalog_name}.{schema_name}.test_set")

# Select the necessary columns for prediction and target
X_test = test_set.select(num_features + cat_features).toPandas()
y_test = test_set.select(target).toPandas()

log.info("Generating predictions for old and new model...")

# Generate predictions from both models
y_pred_old = existing_model.predict(X_test)
y_pred_new = new_model.predict(X_test)

log.info("Comparing metrics...")

old_accuracy = accuracy_score(y_test, y_pred_old)
old_precision = precision_score(y_test, y_pred_old)
old_recall = recall_score(y_test, y_pred_old)

new_accuracy = accuracy_score(y_test, y_pred_new)
new_precision = precision_score(y_test, y_pred_new)
new_recall = recall_score(y_test, y_pred_new)

log.info(f"Old model accuracy: {old_accuracy} | New model accuracy: {new_accuracy}")
log.info(f"Old model precision: {old_precision} | New model precision: {new_precision}")
log.info(f"Old model recall: {old_recall} | New model recall: {new_recall}")


if new_accuracy > old_accuracy:
    log.info("New model is better based on accuracy.")
    model_version = mlflow.register_model(
        model_uri=new_model_uri,
        name=f"{catalog_name}.{schema_name}.hotel_cancels_model",
        tags={"git_sha": f"{git_sha}", "job_run_id": job_run_id},
    )

    log.info("New model registered with version:", model_version.version)
    dbutils.jobs.taskValues.set(key="model_version", value=model_version.version)
    dbutils.jobs.taskValues.set(key="model_update", value=1)
else:
    log.info("Old model is better based on accuracy.")
    dbutils.jobs.taskValues.set(key="model_update", value=0)
