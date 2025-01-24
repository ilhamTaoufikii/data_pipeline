from pipeline import run_pipeline
from config import FILE_PATH

def main():
    """
    Main function to execute the data pipeline with step-by-step error handling.
    """
    try:
        file_path = FILE_PATH
        print(f"Starting pipeline with file: {file_path}")

        if not file_path:
            raise ValueError("The file path is empty or not defined.")
        print("File path validation passed.")

        try:
            run_pipeline(file_path)
            print("Pipeline executed successfully.")
        except Exception as e:
            raise RuntimeError(f"Error during pipeline execution: {e}")

        print("All steps completed successfully.")

    except ValueError as val_error:
        print(f"ValueError: {val_error}")
    except RuntimeError as run_error:
        print(f"RuntimeError: {run_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
