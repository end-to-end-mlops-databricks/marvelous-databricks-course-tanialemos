import yaml
from databricks.connect import DatabricksSession

from src import logger, preprocessing

log = logger.Logger(__name__)

# Load configuration
log.info("Loading configuration...")

with open("project_config.yml", "r") as file:
    config = yaml.safe_load(file)

log.info(f"Configuration loaded:\n {yaml.dump(config, default_flow_style=False)}")


# Build Databricks session
log.info("Getting Databricks session...")

try:
    spark = DatabricksSession.builder.profile(
        "dbc-643c4c2b-d6c9"
    ).getOrCreate()  # databrickscfg profile host and cluster must match workspace
except Exception as e:
    log.error(e)

log.info("Databricks session loaded.")


# Data load and preprocessing
log.info("Start data preprocessing...")

db_filepath = "/Volumes/mlops_students/tanialemosribeiro/data/hotel-reservations.csv"
preprocessor = preprocessing.Preprocessor(config, spark, db_filepath)
preprocessor.preprocess_and_save_data()


log.info("Data preprocessing finished")
