# config/hdfs_config.py

from hdfs import InsecureClient

HDFS_NAMENODE_URL = "http://localhost:9870"

def get_hdfs_client():
    """
    Initialize and return an HDFS client instance.
    """
    return InsecureClient(HDFS_NAMENODE_URL, user="root")

hdfs_client = get_hdfs_client()