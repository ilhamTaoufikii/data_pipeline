# data_pipeline
This project implements a data pipeline to process historical vaccination data, including missing value handling, data transformation
The processed data is then stored in BigQuery for further analysis. The pipeline is designed to be modular and reusable, with the aim of providing valuable insights into vaccination trends


# data_pipeline/
├── src/
│   ├── data/
│   │   └── Données sources
│   ├── __init__.py
│   ├── config.py               # Configuration des paramètres
│   ├── pipeline.py             # This file orchestrates the entire pipeline
│   ├── utils.py                # This file contains utility functions for data loading
│   ├── app.py                  # This file is esigned to trigger the process
│   └── app_test.py             # This file contains unit tests for the functions in utils.py
├── requirements.txt            # Python Dependencies
└── README.md                   # Project Documentation

# Functions Descriptions
# load_data(file_path)
Loads the CSV file from the specified file path into a Pandas DataFrame
# preprocess_data(df)
Handles missing values and performs basic data cleaning on the DataFrame, such as filling or removing NaNs.
# store_data_to_bigquery(df, table_name)
Stores the processed data into a specified BigQuery table for further analysis

These functions work together to create a data pipeline that loads, processes, splits, and stores historical data