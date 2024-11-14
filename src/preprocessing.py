from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler
import pandas as pd
import re

from . import logger

logger = logger.Logger(__name__)

class Preprocessor:

    def __init__(self, config: dict, spark: SparkSession, filepath: str) -> None:
        self.config: dict = config
        self.sparksession: SparkSession = spark
        self.filepath: str = filepath
        self.logger = logger
        self.raw_df: DataFrame = None
        self.preprocessed_df: pd.DataFrame = None


    def __load_data(self) -> DataFrame:
        self.logger.info("Data loading...")

        try:
            self.raw_df = self.sparksession.read.csv(self.filepath, header=True)
        except Exception as e:
            self.logger.error(e)

        self.logger.info(f"Databricks df shape: {(self.raw_df.count(), len(self.raw_df.columns))}")

        return self.raw_df


    def __clean_and_preprocess_data(self) -> pd.DataFrame:
        self.logger.info("Data cleaning and preprocessing...")

        self.logger.info("Print schema")
        self.raw_df.printSchema()

        self.logger.info("Check unique Booking Status values")
        self.raw_df.select("booking_status").distinct().show()

        self.raw_df = self.raw_df.dropDuplicates()
        self.logger.info(f"DF shape after dropDuplicates: {(self.raw_df.count(), len(self.raw_df.columns))}")

        # Select specified features and target
        df_pd = self.raw_df.select(self.config["num_features"] + self.config["cat_features"] + [self.config["target"]]).toPandas()

        # Map target variable values
        df_pd[self.config["target"]] = df_pd[self.config["target"]].map({'Canceled': 1, 'Not_Canceled': 0})

        # Create preprocessing steps
        numeric_transformer = Pipeline(steps=[("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())])

        categorical_transformer = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
                ("onehot", OneHotEncoder(sparse_output=False, handle_unknown="ignore")), # sparse_output=False to ensure dense
            ]
        )

        # Combine preprocessing steps
        preprocessor = ColumnTransformer(
            transformers=[
                ("num", numeric_transformer, self.config["num_features"]),
                ("cat", categorical_transformer, self.config["cat_features"]),
                ("target", 'passthrough', [self.config["target"]])
            ],
            #sparse_threshold = 0 # to always return dense
        )

        self.logger.info("Transform pipeline...")
        preprocessor.set_output(transform="pandas")
        self.preprocessed_df = preprocessor.fit_transform(df_pd)

        # Remove prefixes and invalid characters from column names
        self.preprocessed_df.columns = self.preprocessed_df.columns.str.replace(r'^(num__|cat__|target__)', '', regex=True)
        self.preprocessed_df.columns = self.preprocessed_df.columns.str.replace(r'[^A-Za-z0-9_]', '_', regex=True)
                
        self.logger.info(f"Preprocessed df shape: {self.preprocessed_df.shape}")

        self.logger.info("Finished data cleaning and preprocessing")

        return self.preprocessed_df


    def __split_data(self) -> list:
        self.logger.info("Spliting data...")
        test_size = self.config["test_size"]
        target = self.config["target"]
        return train_test_split(self.preprocessed_df, test_size=test_size, random_state=42, stratify=self.preprocessed_df[target])
    

    def __save_to_catalog(self, train_set: pd.DataFrame, test_set: pd.DataFrame) -> None:
        self.logger.info("Saving train and test sets to Catalog...")

        # Create spark df and add timestamp column to mimic data ingestion in the future
        train_set_with_timestamp = self.sparksession.createDataFrame(train_set).withColumn(
            "update_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC"))   
        
        test_set_with_timestamp = self.sparksession.createDataFrame(test_set).withColumn(
            "update_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC"))

        train_set_with_timestamp.write.mode("append").saveAsTable(
            f"{self.config['catalog_name']}.{self.config['schema_name']}.train_set")
        
        test_set_with_timestamp.write.mode("append").saveAsTable(
            f"{self.config['catalog_name']}.{self.config['schema_name']}.test_set")

        self.sparksession.sql(f"ALTER TABLE {self.config['catalog_name']}.{self.config['schema_name']}.train_set "
          "SET TBLPROPERTIES (delta.enableChangeDataFeed = true);")
        
        self.sparksession.sql(f"ALTER TABLE {self.config['catalog_name']}.{self.config['schema_name']}.test_set "
          "SET TBLPROPERTIES (delta.enableChangeDataFeed = true);")



    def preprocess_and_save_data(self)-> None:
        self.__load_data()
        self.__clean_and_preprocess_data()
        train_set, test_set = self.__split_data()        
        self.__save_to_catalog(train_set, test_set)

        return
