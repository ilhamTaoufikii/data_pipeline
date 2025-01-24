import unittest
import pandas as pd
from utils import load_data, preprocess_data, split_data
from config import FILE_PATH

class TestDataPipeline(unittest.TestCase):
    """
    Unit tests for the data pipeline functions.
    """

    def test_load_data(self):
        """
        Test if data is loaded correctly.
        """
        df = load_data(FILE_PATH)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0, "Dataframe should not be empty")

    def test_preprocess_data(self):
        """
        Test if missing values are handled correctly.
        """
        df = pd.DataFrame({'col1': [1, 2, None], 'col2': [4, None, 6]})
        df_processed = preprocess_data(df)
        self.assertFalse(df_processed.isnull().values.any(), "There should be no missing values")

    def test_split_data(self):
        """
        Test the data splitting into train and test sets.
        """
        df = pd.DataFrame({'col1': [1, 2, 3, 4, 5]})
        train_data, test_data = split_data(df, test_size=0.4)
        self.assertEqual(len(train_data), 3)
        self.assertEqual(len(test_data), 2)

if __name__ == "__main__":
    unittest.main()
