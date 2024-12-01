import pytest

from app.map_reduce.map_reduce_rating import map_reduce
from app.repository.hdfs_repository import create_directory, list_directory, delete, upload_file, read_path, write_path
from app.settings.config import hdfs_client


@pytest.fixture(scope="function")
def clean_up_fixture():
    directories_to_clean = list_directory("/")
    for directory in directories_to_clean:
        delete(f"/{directory}", recursive=True)
    yield
    directories_to_clean = list_directory("/")
    for directory in directories_to_clean:
        delete(f"/{directory}", recursive=True)

@pytest.fixture(scope="function")
def directory_fixture(clean_up_fixture):
    hdfs_directory = "/data"
    create_directory(hdfs_directory)
    return hdfs_directory

def test_map_reduce(directory_fixture):
    # Upload file
    input_path = f"{directory_fixture}/u.data"
    local_file_path = "../data/ml-100k/u.data"

    upload_file(input_path,local_file_path)

    data = read_path(f"{directory_fixture}/u.data")

    res = map_reduce(data)

    output_content = "\n".join(res)

    write_path(f"{directory_fixture}/u2.data", output_content)

    # Read and print file content
    with hdfs_client.read(f"{directory_fixture}/u2.data") as reader:
        content = reader.read().decode('utf-8')
        # Print first 5 lines
        print("\nFirst 5 lines of content:")
        for line in content.splitlines()[:5]:
            print(line)

        # Print total number of lines
        print(f"\nTotal number of lines: {len(content.splitlines())}")