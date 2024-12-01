import toolz as t
from app.settings.config import hdfs_client

def list_directory(path: str):
    return hdfs_client.list(path)

def create_directory(path: str):
    hdfs_client.makedirs(path)

@t.curry
def upload_file(hdfs_path: str, local_path: str):
    hdfs_client.upload(hdfs_path, local_path)

@t.curry
def download_file(hdfs_path: str, local_path: str):
    hdfs_client.download(hdfs_path, local_path)


def delete(path: str, recursive: bool = True):
    if hdfs_client.delete(path, recursive=recursive):
        print(f"Deleted: {path}")
    else:
        print(f"Failed to delete: {path}")

def read_path(input_path: str):
    with hdfs_client.read(input_path) as reader:
        return [line.decode('utf-8').strip() for line in reader]

@t.curry
def write_path(output_path: str, output_content: str):
    with hdfs_client.write(output_path) as writer:
        writer.write(output_content.encode('utf-8'))