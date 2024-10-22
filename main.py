import yaml
from databricks.connect import DatabricksSession

from src import preprocessing

# Load configuration
with open("project_config.yml", "r") as file:
    config = yaml.safe_load(file)

print("Configuration loaded:")
print(yaml.dump(config, default_flow_style=False))

# Build Databricks session
spark = DatabricksSession.builder.profile(
    "dbc-643c4c2b-d6c9"
).getOrCreate()  # databrickscfg profile host and cluster must match workspace

# Simple data load and preprocessing
db_filepath = "/Volumes/mlops_students/tanialemosribeiro/data/hotel-reservations.csv"

df = preprocessing.load_and_preprocess(spark, db_filepath)

df.show(5)
