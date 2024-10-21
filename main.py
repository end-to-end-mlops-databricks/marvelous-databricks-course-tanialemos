from src import preprocessing
import yaml

# Load configuration
with open("project_config.yml", "r") as file:
    config = yaml.safe_load(file)

print("Configuration loaded:")
print(yaml.dump(config, default_flow_style=False))

# Simple data load
local_filepath = "data\\hotel-reservations.csv"
db_filepath = "/Volumes/mlops_students/tanialemosribeiro/data/hotel-reservations.csv"

preprocessing.load_local_data(local_filepath)

preprocessing.load_vol_data(db_filepath)

