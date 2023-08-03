# test_data_processor.py
import sys
import os

# Add the root directory (sapple) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import unittest
from unittest.mock import patch, Mock
from src.utils.data_processor import read_data_from_table

class TestDataProcessor(unittest.TestCase):

    def setUp(self):
        # Create a mock SparkSession for each test case
        self.mock_spark = Mock()

    def test_read_data_from_table(self):
        # Set up the mock SparkSession behavior
        mock_read = self.mock_spark.read
        mock_format = mock_read.format.return_value
        mock_options = mock_format.options.return_value
        mock_load = mock_options.load.return_value

        # Set up the mock behavior to return test data when the load() method is called
        test_data = [('Alice', 25), ('Bob', 30)]
        mock_load.collect.return_value = test_data

        # Replace SparkSession.builder.getOrCreate with the mock SparkSession
        with patch('src.utils.data_processor.SparkSession.builder.getOrCreate', return_value=self.mock_spark):
            # Call the method under test
            table_name = 'users'
            jdbc_url = 'jdbc:mysql://localhost:3306/mydb'
            result_df = read_data_from_table(table_name, jdbc_url)

        # Assertions
        # Verify that SparkSession.builder.getOrCreate was called once
        # data_processor.SparkSession.builder.getOrCreate.assert_called_once()

        # Verify that SparkSession.read.format was called once with 'jdbc' format
        mock_read.format.assert_called_once_with("jdbc")

        # Verify that the options were set correctly with the JDBC URL and table name
        mock_options.assert_called_once_with(url=jdbc_url, dbtable=table_name)

        # Verify that the load() method was called once
        mock_load.collect.assert_called_once()

        # Check the result DataFrame (optional, depends on your test case)
        self.assertEqual(result_df.collect(), test_data)

    def test_read_data_from_table_empty_result(self):
        # Set up the mock behavior to return an empty DataFrame when the load() method is called
        mock_read = self.mock_spark.read
        mock_format = mock_read.format.return_value
        mock_options = mock_format.options.return_value
        mock_load = mock_options.load.return_value
        mock_load.collect.return_value = []

        # Replace SparkSession.builder.getOrCreate with the mock SparkSession
        with patch('src.utils.data_processor.SparkSession.builder.getOrCreate', return_value=self.mock_spark):
            # Call the method under test with a different table_name
            table_name = 'empty_table'
            jdbc_url = 'jdbc:mysql://localhost:3306/mydb'
            result_df = read_data_from_table(table_name, jdbc_url)

        # Assertions
        # Verify that SparkSession.builder.getOrCreate was called once
        # data_processor.SparkSession.builder.getOrCreate.assert_called_once()

        # Verify that SparkSession.read.format was called once with 'jdbc' format
        mock_read.format.assert_called_once_with("jdbc")

        # Verify that the options were set correctly with the JDBC URL and table name
        mock_options.assert_called_once_with(url=jdbc_url, dbtable=table_name)

        # Verify that the load() method was called once
        mock_load.collect.assert_called_once()

        # Check that the result DataFrame is empty
        self.assertEqual(result_df.count(), 0)

if __name__ == '__main__':
    unittest.main()
