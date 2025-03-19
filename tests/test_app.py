import pytest
import polars as pl
from extractor.app import loader, extractor
from requests.exceptions import RequestException


def test_loader_success(tmp_path):
    # Create a temporary directory and file path
    temp_file = tmp_path / "test.parquet"

    # Create a sample polars DataFrame
    df = pl.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})

    # Call the loader function
    loader(df, str(temp_file))

    # Assert that the file was created
    assert temp_file.exists()

    # Read the file back and verify its contents
    loaded_df = pl.read_parquet(temp_file)
    assert df.frame_equal(loaded_df)


def test_loader_failure(mocker):
    # Mock the write_parquet method to raise an exception
    mock_df = mocker.MagicMock()
    mock_df.write_parquet.side_effect = Exception("Mocked exception")

    # Assert that SystemExit is raised when an exception occurs
    with pytest.raises(SystemExit):
        loader(mock_df, "invalid_path.parquet")


def test_extractor_success(mocker):
    # Mock the requests.get method to return a successful response
    mock_response = mocker.Mock()
    mock_response.json.return_value = [{"column1": 1, "column2": "a"}, {"column1": 2, "column2": "b"}]
    mocker.patch("requests.get", return_value=mock_response)

    # Call the extractor function
    url = "http://mocked-url.com"
    df = extractor(url)

    # Assert that the returned DataFrame matches the expected data
    expected_df = pl.DataFrame({"column1": [1, 2], "column2": ["a", "b"]})
    assert df.frame_equal(expected_df)


def test_extractor_api_down(mocker):
    # Mock the requests.get method to raise a RequestException
    mocker.patch("requests.get", side_effect=RequestException("API is down"))

    # Assert that SystemExit is raised when the API is down
    url = "http://mocked-url.com"
    with pytest.raises(SystemExit):
        extractor(url)


def test_extractor_data_conversion_failure(mocker):
    # Mock the requests.get method to return invalid JSON data
    mock_response = mocker.Mock()
    mock_response.json.return_value = "invalid_data"
    mocker.patch("requests.get", return_value=mock_response)

    # Assert that SystemExit is raised when data conversion fails
    url = "http://mocked-url.com"
    with pytest.raises(SystemExit):
        extractor(url)
