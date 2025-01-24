import pandas as pd
from config import FILE_PATH, PROJECT_ID, DATASET_ID, TABLE_ID
from google.cloud import bigquery

import pandas as pd

def load_data(file_path):
    """
    Load the dataset from the given file path.
    """
    try:
        df = pd.read_csv(file_path, sep=';')
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def preprocess_data(df):
    """
    Perform basic data preprocessing like handling missing values
    by filling them with the mean for numerical columns.
    """
    try:

        numeric_cols = df.select_dtypes(include=['number']).columns
        print(f"Numeric columns identified: {numeric_cols}")

        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())

        print("Missing values handled.")
        print(f"Data after preprocessing:\n{df.head()}")
        return df

    except Exception as e:
        print(f"Error in preprocessing: {e}")
        return None

def split_data(df, test_size=0.2):
    """
    Split the data into training and testing sets.
    """
    try:
        train_size = int((1 - test_size) * len(df))
        train_data = df[:train_size]
        test_data = df[train_size:]
        print("Data split into train and test sets.")
        print(f"Train data sample:\n{train_data.head()}")
        print(f"Test data sample:\n{test_data.head()}")
        return train_data, test_data
    except Exception as e:
        print(f"Error in splitting data: {e}")
        return None, None

def store_data_to_bigquery(df, project_id, dataset_id, table_id):
    """
    Store the DataFrame to a BigQuery table using the google-cloud-bigquery library.
    """
    try:
        print(df.dtypes)
        client = bigquery.Client(project=project_id)
        
        table_full_id = f"{project_id}.{dataset_id}.{table_id}"
        table_ref = client.dataset(dataset_id).table(table_id)
        
        rows_to_insert = df.to_dict(orient='records')
        print(f"Attempting to insert {len(rows_to_insert)} rows into BigQuery table: {table_full_id}")
        errors = client.insert_rows_json(table_ref, rows_to_insert)
        
        if errors:
            print(f"Errors occurred while inserting data: {errors}")
        else:
            print(f"Data successfully stored in BigQuery table: {table_full_id}")
    except Exception as e:
        print(f"Error storing data to BigQuery: {e}")
