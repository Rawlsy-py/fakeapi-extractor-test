import pytest
import requests
from unittest.mock import patch, MagicMock
from extractor.app import upload_to_dospace
import polars as pl

@patch("extractor.app.requests.get")
@patch("extractor.app.session.client")
def test_upload_to_dospace_success(mock_s3_client, mock_requests_get):
    # Mock the API response
    mock_requests_get.return_value.json.return_value = {"key": "value"}
    mock_requests_get.return_value.status_code = 200

    # Mock the S3 client
    mock_s3 = MagicMock()
    mock_s3_client.return_value = mock_s3

    # Call the function
    url = "http://example.com/api"
    df = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    bucket_name = "test-bucket"
    object_name = "test-object"
    upload_to_dospace(url, df, bucket_name, object_name)

    # Assertions
    mock_requests_get.assert_called_with(url)
    mock_s3.put_object.assert_called_once_with(
        Bucket=bucket_name,
        Key=object_name,
        Body={"key": "value"},
        ContentType="application/json",
    )


@patch("extractor.app.requests.get")
def test_upload_to_dospace_api_down(mock_requests_get):
    # Mock the API to raise an exception
    mock_requests_get.side_effect = requests.exceptions.RequestException("API is down")

    # Call the function and assert it raises SystemExit
    url = "http://example.com/api"
    df = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    bucket_name = "test-bucket"
    object_name = "test-object"

    with pytest.raises(SystemExit):
        upload_to_dospace(url, df, bucket_name, object_name)


@patch("extractor.app.requests.get")
@patch("extractor.app.session.client")
def test_upload_to_dospace_upload_failure(mock_s3_client, mock_requests_get):
    # Mock the API response
    mock_requests_get.return_value.json.return_value = {"key": "value"}
    mock_requests_get.return_value.status_code = 200

    # Mock the S3 client to raise an exception
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("Upload failed")
    mock_s3_client.return_value = mock_s3

    # Call the function and assert it raises SystemExit
    url = "http://example.com/api"
    df = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    bucket_name = "test-bucket"
    object_name = "test-object"

    with pytest.raises(SystemExit):
        upload_to_dospace(url, df, bucket_name, object_name)