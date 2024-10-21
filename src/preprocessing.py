from databricks.connect import DatabricksSession
import pandas as pd

def print_hello():
    print("hello from preprocessing!")

def load_local_data(filepath):
    try:
        df = pd.read_csv(filepath)
        print(f"Local df shape: {df.shape}")
    except Exception as e:
        print(f"Yikes! An error ocurred: {e}")
    

def load_vol_data(filepath):
    try:
        spark = DatabricksSession.builder.profile("dbc-643c4c2b-d6c9").getOrCreate() # databrickscfg profile host and cluster must match workspace
        df = spark.read.csv(filepath)
        print(f"Databricks df shape: {(df.count(), len(df.columns))}")
    except Exception as e:
        print(f"Yikes! An error ocurred: {e}")
    