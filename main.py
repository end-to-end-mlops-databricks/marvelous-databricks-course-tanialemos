import yaml
from databricks.connect import DatabricksSession

from src import preprocessing
from src.logger import Logger

log = Logger(__name__)

# Load configuration
log.info("Loading configuration...")

with open("project_config.yml", "r") as file:
    config = yaml.safe_load(file)

log.info("Configuration loaded:")
print(yaml.dump(config, default_flow_style=False))


# Build Databricks session
log.info("Getting Databricks session...")

spark = DatabricksSession.builder.profile(
    "dbc-643c4c2b-d6c9"
).getOrCreate()  # databrickscfg profile host and cluster must match workspace

log.info("Databricks session loaded.")


# Simple data load and preprocessing
db_filepath = "/Volumes/mlops_students/tanialemosribeiro/data/hotel-reservations.csv"

X_train, X_test, y_train, y_test = preprocessing.load_and_preprocess(spark, db_filepath, config)

log.info("X train:")
log.info(X_train[1])
log.info("y train:")
log.info(y_train[:5])
