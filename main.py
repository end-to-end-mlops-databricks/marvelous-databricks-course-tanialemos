import yaml
from databricks.connect import DatabricksSession

from src import logger, model_training, preprocessing

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

# Train and log model with Unity Catalog data
log.info("Start model training and logging...")

model_training = model_training.ModelTraining(config, spark)
model_training.train_and_log_model()

log.info("Model training and logging finished")

# Register a model in Unity Catalog
log.info("Start model registration...")

model_training = model_training.ModelTraining(config, spark)
model_training.register_model("eae6c929409544788c337eeea5d2b3c1", "lr-hotel-cancels-lbfgs")  # hard-coded for now

log.info("Model registration finished")
