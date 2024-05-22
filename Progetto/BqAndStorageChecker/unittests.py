import unittest
from unittest.mock import MagicMock, patch
from classes.BqChecker import BigQueryChecker
from classes.StorageChecker import StorageChecker


class TestStorageChecker(unittest.TestCase):
    @patch('classes.StorageChecker.storage.Client')
    def test_blob_exists(self, mock_storage_client):
        # Setup
        checker = StorageChecker()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.get_bucket.return_value = mock_bucket

        # Test
        result = checker.blob_exists('test_bucket', 'test_blob')

        # Assert
        self.assertTrue(result)
        mock_storage_client.return_value.get_bucket.assert_called_once_with('test_bucket')
        mock_bucket.blob.assert_called_once_with('test_blob')
        mock_blob.exists.assert_called_once()


class TestBigQueryChecker(unittest.TestCase):
    @patch('classes.BqChecker.bigquery.Client')
    def test_table_exists(self, mock_bigquery_client):
        # Setup
        checker = BigQueryChecker('test_dataset')
        mock_dataset = MagicMock()
        mock_table = MagicMock()
        mock_bigquery_client.return_value.dataset.return_value = mock_dataset
        mock_dataset.table.return_value = mock_table
        mock_bigquery_client.return_value.get_table.return_value = mock_table

        # Test
        result = checker.table_exists('test_table')

        # Assert
        self.assertTrue(result)
        mock_bigquery_client.return_value.dataset.assert_called_once_with('test_dataset')
        mock_dataset.table.assert_called_once_with('test_table')
        mock_bigquery_client.return_value.get_table.assert_called_once_with(mock_table)


if __name__ == '__main__':
    unittest.main()

