import pytest
import os

from app.repository.hdfs_repository import delete, create_directory, list_directory, upload_file, download_file


@pytest.fixture(scope="module")
def clean_up_fixture():
    directories_to_clean = list_directory("/")
    for directory in directories_to_clean:
        delete(f"/{directory}", recursive=True)
    yield


@pytest.fixture(scope="module")
def directory_fixture(clean_up_fixture):
    hdfs_directory = "/test_directory"
    create_directory(hdfs_directory)
    return hdfs_directory

def test_create_directory(directory_fixture):
    """
      Test Creating a directory in HDFS.
      """
    directory_name = directory_fixture.replace("/", "")
    # Assert that the directory exists
    assert (
        directory_name in list_directory("/"),
        "The directory was not created in HDFS."
    )


def test_upload_file(directory_fixture):
    """
      Test uploading a file to HDFS using the pre-created directory.
      """
    hdfs_directory = directory_fixture
    hdfs_file_path = f"{hdfs_directory}/uploaded_file.txt"
    local_file_path = "test_file.txt"

    # Step 1: Create a local file for upload
    with open(local_file_path, "w") as f:
        f.write("This is a test file for HDFS upload.")

    # Step 2: Upload the file
    upload_file(hdfs_file_path, local_file_path)

    # Step 3: Assert that the file exists in HDFS
    assert (
        "uploaded_file.txt" in list_directory(hdfs_directory),
        "The file was not uploaded to HDFS."
    )

    # Cleanup: Remove the local file
    if os.path.exists(local_file_path):
        os.remove(local_file_path)


def test_download_file(directory_fixture):
    """
    Test downloading a file from HDFS to the local filesystem.
    """
    hdfs_directory = directory_fixture
    hdfs_file_path = f"{hdfs_directory}/uploaded_file.txt"
    local_upload_file_path = "upload_test_file.txt"
    local_download_file_path = "downloaded_test_file.txt"

    # Step 1: Create and upload a file to HDFS
    with open(local_upload_file_path, "w") as f:
        f.write("This is a test file for HDFS download.")

    upload_file(hdfs_file_path, local_upload_file_path)

    # Step 2: Download the file from HDFS
    download_file(hdfs_file_path, local_download_file_path)

    # Step 3: Assert that the downloaded file exists locally
    assert (
        os.path.exists(local_download_file_path),
        "The downloaded file does not exist locally."
    )

    # Step 4: Assert the content of the downloaded file matches the original
    with open(local_upload_file_path, "r") as original:
        original_content = original.read()

    with open(local_download_file_path, "r") as downloaded:
        downloaded_content = downloaded.read()

    assert (
        original_content == downloaded_content,
        "The downloaded file's content does not match the original."
    )

    # Cleanup: Remove local files
    if os.path.exists(local_upload_file_path):
        os.remove(local_upload_file_path)

    if os.path.exists(local_download_file_path):
        os.remove(local_download_file_path)