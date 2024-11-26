"""
!! Run this script from the UI !!
This script will generate new synthetic data in order to mimic data ingestion.
The synthetic data is added to the existing train set for subsequent model training.
"""

"""
# Load configuration
with open("../project_config.yml", "r") as file:
    config = yaml.safe_load(file)

catalog_name = config["catalog_name"]
schema_name = config["schema_name"]


# Load train and test sets
train_set = spark.table(f"{catalog_name}.{schema_name}.train_set").toPandas()
test_set = spark.table(f"{catalog_name}.{schema_name}.test_set").toPandas()
combined_set = pd.concat([train_set, test_set], ignore_index=True)
combined_set['id'] = [str(i) for i in range(1, len(combined_set) + 1)]  # train and test sets do not have ids so we generate them here.
existing_ids = combined_set["id"].astype(int).to_list()

# Define function to create synthetic data without random state
def create_synthetic_data(df, num_rows=100):
    synthetic_data = pd.DataFrame()

    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]) and column != 'id':
            if column in ['no_of_adults', 'no_of_children', 'no_of_weekend_nights', 'arrival_year', 'arrival_month', 'arrival_date']:
                synthetic_data[column] = np.random.randint(df[column].min(), df[column].max() + 1, num_rows)  # Years between existing values
            else:
                mean, std = df[column].mean(), df[column].std()
                synthetic_data[column] = np.random.normal(mean, std, num_rows)

        elif pd.api.types.is_categorical_dtype(df[column]) or pd.api.types.is_object_dtype(df[column]):
            synthetic_data[column] = np.random.choice(df[column].unique(), num_rows,
                                                      p=df[column].value_counts(normalize=True))

        elif isinstance(df[column].dtype, pd.CategoricalDtype) or isinstance(df[column].dtype, pd.StringDtype):
            synthetic_data[column] = np.random.choice(df[column].unique(), num_rows,
                                                      p=df[column].value_counts(normalize=True))
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            min_date, max_date = df[column].min(), df[column].max()
            if min_date < max_date:
                synthetic_data[column] = pd.to_datetime(
                    np.random.randint(min_date.value, max_date.value, num_rows)
                )
            else:
                synthetic_data[column] = [min_date] * num_rows

        else:
            synthetic_data[column] = np.random.choice(df[column], num_rows)

    new_ids = []
    i = max(existing_ids) + 1 if existing_ids else 1
    while len(new_ids) < num_rows:
        if i not in existing_ids:
            new_ids.append(str(i))  # ensure id is string
        i += 1
    synthetic_data['id'] = new_ids

    return synthetic_data

# Create synthetic data
synthetic_df = create_synthetic_data(combined_set)

#if table_exists:
#    existing_schema = spark.table(f"{catalog_name}.{schema_name}.source_data").schema
#    synthetic_spark_df = spark.createDataFrame(synthetic_df, schema=existing_schema)

synthetic_spark_df = spark.createDataFrame(synthetic_df)

train_set_with_timestamp = synthetic_spark_df.withColumn(
    "update_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC")
)

# Append synthetic data as new data to source_data table
train_set_with_timestamp.write.mode("append").saveAsTable(
    f"{catalog_name}.{schema_name}.source_data"
)"""
