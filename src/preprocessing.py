from pyspark.sql import functions as F
from pyspark.sql.functions import col
import pandas as pd


def load_data(spark, filepath):
    print("--- Start data loading ---")

    try:
        df = spark.read.csv(filepath, header=True)
    except Exception as e:
        print(f"Yikes! An error ocurred: {e}")

    print(f"Databricks df shape: {(df.count(), len(df.columns))}")
    print("--- Finhished data loading ---")

    return df


def clean_and_preprocess_data(df):
    print("--- Start data cleaning and preprocessing ---")

    print("--- Print schema ---")
    df.printSchema()

    print("---  Check unique Booking Status values ---")
    df.select("booking_status").distinct().show()

    df = df.dropDuplicates()
    print(f"DF shape after dropDuplicates: {(df.count(), len(df.columns))}")

    # TODO fill na
    # TODO encode cat vars

    df = df.withColumn("target", 
                            F.when(df.booking_status == "Not_Canceled", 0)
                            .when(df.booking_status == "Canceled", 1))

    cleaned_df = df.drop("Booking_ID", "booking_status")

    print(f"DF shape after clean and preprocess: {(cleaned_df.count(), len(cleaned_df.columns))}")
    print("--- Finished data cleaning and preprocessing ---")

    return cleaned_df


def load_and_preprocess(spark, filepath):

    df = load_data(spark, filepath)
    df = clean_and_preprocess_data(df)

    return df

