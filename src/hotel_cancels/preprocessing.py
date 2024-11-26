import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from . import logger

logger = logger.Logger(__name__)


class Preprocessor:
    """
    A class to preprocess and transform data using PySpark and Scikit-learn pipelines.

    Attributes:
        - config (dict): Configuration dictionary with the necessary parameters for preprocessing.
        - sparksession (SparkSession): A Spark session object to work with Spark DataFrames.
        - filepath (str): Path to the input data file.
        - logger (Logger): Logger instance to log relevant information during preprocessing.
        - raw_df (DataFrame): Raw Spark DataFrame loaded from the file.
        - preprocessed_df (pd.DataFrame): Processed Pandas DataFrame after transformations.
    """

    def __init__(self, config: dict, spark: SparkSession, filepath: str | None, df: DataFrame | None) -> None:
        """
        Initialize the Preprocessor class with configuration, Spark session, and file path.

        Args:
            - config (dict): Configuration dictionary with features and target settings.
            - spark (SparkSession): Spark session to interact with the Spark cluster.
            - filepath (str): Path to the input data file.
        """
        self.config: dict = config
        self.sparksession: SparkSession = spark
        self.filepath: str = filepath
        self.df: DataFrame = df
        self.logger = logger
        self.raw_df: DataFrame = None
        self.preprocessed_df: pd.DataFrame = None

    def __load_data(self) -> DataFrame:
        """
        Load the data from the specified filepath into a Spark DataFrame.

        This method reads a CSV file and logs relevant information such as the DataFrame shape
        and column names.

        Returns:
            - DataFrame: The raw Spark DataFrame loaded from the file.
        """
        self.logger.info("Data loading...")

        try:
            self.raw_df = self.sparksession.read.csv(self.filepath, header=True)
        except Exception as e:
            self.logger.error(e)

        self.logger.info(f"Databricks df shape: {(self.raw_df.count(), len(self.raw_df.columns))}")

        return self.raw_df

    def __clean_and_preprocess_data(self) -> pd.DataFrame:
        """
        Clean and preprocess the raw data.

        This method includes:
            - Dropping duplicates
            - Selecting relevant features and target
            - Applying preprocessing steps (imputation, scaling, encoding)
            - Cleaning column names (removing prefixes and invalid characters)

        Returns:
            - pd.DataFrame: The preprocessed Pandas DataFrame.log.info(f"{affected_rows_test} new rows were added to test set.")

        """
        self.logger.info("Data cleaning and preprocessing...")

        self.logger.info("Print schema")
        self.raw_df.printSchema()

        self.logger.info("Check unique Booking Status values")
        self.raw_df.select("booking_status").distinct().show()

        self.raw_df = self.raw_df.dropDuplicates()
        self.logger.info(f"DF shape after dropDuplicates: {(self.raw_df.count(), len(self.raw_df.columns))}")

        # Select specified features and target
        df_pd = self.raw_df.select(
            self.config["num_features"] + self.config["cat_features"] + [self.config["target"]]
        ).toPandas()

        # Map target variable values
        df_pd[self.config["target"]] = df_pd[self.config["target"]].map({"Canceled": 1, "Not_Canceled": 0})

        # Create preprocessing steps
        numeric_transformer = Pipeline(
            steps=[("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]
        )

        categorical_transformer = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
                (
                    "onehot",
                    OneHotEncoder(sparse_output=False, handle_unknown="ignore"),
                ),  # sparse_output=False to ensure dense
            ]
        )

        # Combine preprocessing steps
        preprocessor = ColumnTransformer(
            transformers=[
                ("num", numeric_transformer, self.config["num_features"]),
                ("cat", categorical_transformer, self.config["cat_features"]),
                ("target", "passthrough", [self.config["target"]]),
            ],
            # sparse_threshold = 0 # to always return dense
        )

        self.logger.info("Transform pipeline...")
        preprocessor.set_output(transform="pandas")
        self.preprocessed_df = preprocessor.fit_transform(df_pd)

        # Remove prefixes and invalid characters from column names
        self.preprocessed_df.columns = self.preprocessed_df.columns.str.replace(
            r"^(num__|cat__|target__)", "", regex=True
        )
        self.preprocessed_df.columns = self.preprocessed_df.columns.str.replace(r"[^A-Za-z0-9_]", "_", regex=True)

        self.logger.info(f"Preprocessed df shape: {self.preprocessed_df.shape}")

        self.logger.info("Finished data cleaning and preprocessing")

        return self.preprocessed_df

    def __split_data(self) -> list:
        """
        Split the preprocessed data into training and testing sets.

        Returns:
            - list: A list containing the train and test sets.
        """
        self.logger.info("Spliting data...")
        test_size = self.config["test_size"]
        target = self.config["target"]
        return train_test_split(
            self.preprocessed_df, test_size=test_size, random_state=42, stratify=self.preprocessed_df[target]
        )

    def __save_to_catalog(self, train_set: pd.DataFrame, test_set: pd.DataFrame) -> None:
        """
        Save the train and test sets to Unity Catalog in Databricks.

        This method writes the processed train and test sets to the corresponding Delta tables and sets the
        required table properties for change data feed.

        Args:
            - train_set (pd.DataFrame): The preprocessed training data.
            - test_set (pd.DataFrame): The preprocessed testing data.
        """
        self.logger.info("Saving train and test sets to Catalog...")

        train_table_path = f"{self.config['catalog_name']}.{self.config['schema_name']}.train_set"
        test_table_path = f"{self.config['catalog_name']}.{self.config['schema_name']}.test_set"

        # Create spark df and add timestamp column to mimic data ingestion in the future
        train_set_with_timestamp = self.sparksession.createDataFrame(train_set).withColumn(
            "update_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC")
        )

        test_set_with_timestamp = self.sparksession.createDataFrame(test_set).withColumn(
            "update_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC")
        )

        train_set_with_timestamp.write.mode("append").saveAsTable(train_table_path)

        test_set_with_timestamp.write.mode("append").saveAsTable(test_table_path)

        self.sparksession.sql(f"ALTER TABLE {train_table_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true);")

        self.sparksession.sql(f"ALTER TABLE {test_table_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true);")

    def preprocess_and_save_data(self) -> None:
        """
        Method to preprocess and save the data.

        This method loads the data, cleans and preprocesses it, splits it into train and
        test sets, and finally saves the datasets to Unity Catalog.
        """

        if self.df is not None:
            if self.filepath is not None:
                logger.error("Provide either filepath of dataframe.")
                return
            else:
                self.raw_df = self.df
        elif self.filepath is not None:
            self.__load_data()
        self.__clean_and_preprocess_data()
        train_set, test_set = self.__split_data()
        self.__save_to_catalog(train_set, test_set)

        return
