from app.settings.config import hdfs_client
from functools import reduce

def map_phase(line: str):
    """
    Map phase: Convert each line into { movie_id, rating }
    Input line format: user_id movie_id rating timestamp
    """
    user_id, movie_id, rating, timestamp = map(int, line.split())
    return { 'movie_id': movie_id, 'rating': rating }


def reduce_phase(mapped_data) -> dict:
    """
    Reduce phase: Sum up counts for each movie_id
    """

    def reducer(acc, n):
        movie_id = n.get("movie_id")
        rating = n.get("rating")
        return { **acc, f"{movie_id}": acc.get(movie_id, []) + [rating] }

    return reduce(reducer, mapped_data, {})


def run_mapreduce(input_path: str, output_path: str):
    """
    Run MapReduce job to count ratings per movie
    """
    # Read input data from HDFS
    with hdfs_client.read(input_path) as reader:
        mapped_data = [
            map_phase(line.decode('utf-8').strip())
            for line in reader
            if line
        ]

    # Run reduce phase
    reduced_data = reduce_phase(mapped_data)

    # Write results to HDFS
    output_lines = [
        f"{movie_id}\t{count}"
        for movie_id, count in sorted(reduced_data.items())
    ]

    output_content = '\n'.join(output_lines)

    with hdfs_client.write(output_path) as writer:
        writer.write(output_content.encode('utf-8'))