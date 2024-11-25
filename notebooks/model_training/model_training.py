# Databricks notebook source
import git
import mlflow
import pandas as pd
from mlflow.models import infer_signature
from pyspark.sql import DataFrame, SparkSession
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score

from src import logger

logger = logger.Logger(__name__)


# COMMAND ----------
class ModelTraining:
    def __init__(self, config: dict, spark: SparkSession) -> None:
        self.config: dict = config
        self.sparksession: SparkSession = spark
        self.logger = logger
        self.train_set_spark: DataFrame = None
        self.X_train: pd.DataFrame = None
        self.X_test: pd.DataFrame = None
        self.y_train: pd.DataFrame = None
        self.y_test: pd.DataFrame = None
        self.train_table_path: str = f"{self.config['catalog_name']}.{self.config['schema_name']}.train_set"
        self.test_table_path: str = f"{self.config['catalog_name']}.{self.config['schema_name']}.test_set"

    # COMMAND ----------
    def __get_repo_info(self) -> dict:
        """
        Retrieve repository information like the current Git SHA and branch.
        """
        repo_info: dict = {}
        repo = git.Repo(search_parent_directories=True)
        repo_info["git_sha"] = repo.head.object.hexsha
        repo_info["branch"] = repo.active_branch
        return repo_info

    # COMMAND ----------
    def __load_and_split_data(self) -> None:
        """
        Load training and testing sets from Databricks tables and further split into training variables and target.
        """
        logger.info("Start data loading from Unity Catalog...")
        self.train_set_spark = self.sparksession.table(self.train_table_path)
        logger.info(f"Finished loading data from {self.train_table_path}")
        train_set = self.train_set_spark.toPandas()
        test_set = self.sparksession.table(self.test_table_path).toPandas()
        logger.info(f"Finished loading data from {self.test_table_path}")

        logger.info("Start data splitting...")
        self.X_train = train_set.drop(columns=["update_timestamp_utc", self.config["target"]])
        self.y_train = train_set[self.config["target"]]

        self.X_test = test_set.drop(columns=["update_timestamp_utc", self.config["target"]])
        self.y_test = test_set[self.config["target"]]

    # COMMAND ----------
    def train_and_log_model(self) -> None:
        """
        Train logistic regression models with different solvers and log results to MLflow.
        """
        self.__load_and_split_data()

        repo_info = self.__get_repo_info()
        git_sha = repo_info["git_sha"]
        current_branch = repo_info["branch"]
        solvers = ["lbfgs", "liblinear", "newton-cg", "newton-cholesky", "sag", "saga"]

        mlflow.set_tracking_uri("databricks")
        mlflow.set_experiment(experiment_name="/Shared/hotel-cancels")  # ideally point to the repo name

        for solver in solvers:
            params = {
                "solver": solver,
                "max_iter": 500,
                "random_state": 123,
            }

            logger.info(f"Start model training with params: {params}")

            # Start an MLflow run
            with mlflow.start_run(tags={"git_sha": f"{git_sha}", "branch": f"{current_branch}"}):
                try:
                    # train and predict
                    logger.info("Start model fit and predict...")
                    lr = LogisticRegression(**params)
                    lr.fit(self.X_train, self.y_train)

                    y_pred = lr.predict(self.X_test)

                    # Calculate metrics
                    accuracy = accuracy_score(self.y_test, y_pred)
                    precision = precision_score(self.y_test, y_pred)
                    recall = recall_score(self.y_test, y_pred)

                    # Log the hyperparameters and metrics
                    logger.info("Start logging with mlflow...")
                    mlflow.log_param("model_type", "sklearn logistic regression")
                    mlflow.log_params(params)
                    mlflow.log_metric("accuracy", accuracy)
                    mlflow.log_metric("precision", precision)
                    mlflow.log_metric("recall", recall)

                    # Infer the model signature
                    signature = infer_signature(model_input=self.X_train, model_output=y_pred)

                    # Log input
                    dataset = mlflow.data.from_spark(
                        self.train_set_spark, table_name=self.train_table_path, version=0
                    )  # dataset version
                    mlflow.log_input(dataset, context="training")

                    # Log the model
                    mlflow.sklearn.log_model(
                        sk_model=lr,
                        artifact_path=f"lr-hotel-cancels-{solver}",
                        signature=signature,
                    )
                except Exception as e:
                    # Log failure for the current solver
                    mlflow.log_param("error", str(e))
                    logger.error(f"Solver {solver} failed with error: {e}")

    # COMMAND ----------
    def register_model(self, run_id: str, artifact_path: str) -> None:
        """
        Registers a selected model to the Databricks Unity Catalog.
        """
        mlflow.set_tracking_uri("databricks")
        mlflow.set_registry_uri("databricks-uc")  # It must be -uc for registering models to Unity Catalog

        # registering hard-coded run_id doesn't seem to work.
        # Need to fetch run_info for experiment.
        runs = mlflow.search_runs(experiment_names=["/Shared/hotel-cancels"], output_format="list")
        for run in runs:
            info_run_id = run.info.run_id
            if info_run_id == run_id:
                mlflow.register_model(
                    model_uri=f"runs:/{info_run_id}/{artifact_path}",
                    name=f"{self.config['catalog_name']}.{self.config['schema_name']}.hotel_cancels_model",
                    tags={"git_sha": "3fd5e5c7d10cf8f4e4964b6252a71640b3ce317f"},  # to be refactored
                )
