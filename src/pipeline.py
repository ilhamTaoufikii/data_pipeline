from utils import load_data, preprocess_data, store_data_to_bigquery
from config import FILE_PATH, PROJECT_ID, DATASET_ID, TABLE_ID
import pandas as pd

def run_pipeline(file_path):
    """
    Runs the data pipeline: load, preprocess, transform, and store, with error handling.
    """
    project_id = PROJECT_ID
    dataset_id = DATASET_ID
    table_id = TABLE_ID

    try:
        print("Loading data...")
        df = load_data(file_path)
        if df is None or df.empty:
            raise ValueError("Failed to load data.")
        print("Data loaded successfully.")

        print("Preprocessing data...")
        print(f"Data after preprocessing: {df.head() if df is not None else 'None'}")
        print(f"Shape of data after preprocessing: {df.shape if df is not None else 'None'}")
        df = preprocess_data(df)
        if df is None or df.empty:
            raise ValueError("Preprocessing failed.")
        print("Data preprocessed successfully.")

        print("Storing data to BigQuery...")
        store_data_to_bigquery(df, project_id, dataset_id, table_id)
        print("Data stored successfully in BigQuery.")

    except ValueError as ve:
        print(f"ValueError: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

