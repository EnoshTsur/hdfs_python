from app.settings.config import hdfs_client
from functools import reduce
from statistics import mean


def map_phase(line: str):
    """
    Map phase: Convert each line into movie_id and rating
    Input line format: user_id movie_id rating timestamp
    """
    try:
        user_id, movie_id, rating, timestamp = map(int, line.split())
        return {'movie_id': movie_id, 'rating': rating}
    except ValueError:
        return None


def reduce_phase(mapped_data) -> dict:
    """
    Reduce phase: Calculate average rating and count for each movie_id
    """

    def reducer(acc, n):
        movie_id = n.get("movie_id")
        rating = n.get("rating")

        current = acc.get(str(movie_id), {'sum': 0, 'count': 0, 'ratings': []})
        current['sum'] += rating
        current['count'] += 1
        current['ratings'].append(rating)
        current['avg'] = current['sum'] / current['count']

        return {**acc, str(movie_id): current}

    return reduce(reducer, mapped_data, {})


def run_mapreduce(input_path: str, output_path: str):
    """
    Run MapReduce job to calculate movie rankings based on average ratings
    """
    # Map phase
    with hdfs_client.read(input_path) as reader:
        mapped_data = [
            map_phase(line.decode('utf-8').strip())
            for line in reader
            if line
        ]
        # Filter out None values from failed mappings
        mapped_data = [x for x in mapped_data if x]

    # Reduce phase
    reduced_data = reduce_phase(mapped_data)

    # Sort movies by average rating (descending)
    sorted_movies = sorted(
        reduced_data.items(),
        key=lambda x: (-x[1]['avg'], -x[1]['count'])  # Sort by avg rating then by count
    )

    # Format output with ranking
    output_lines = [
        f"Rank {idx + 1}: Movie {movie_id}, Average Rating: {stats['avg']:.2f}, "
        f"Number of Ratings: {stats['count']}"
        for idx, (movie_id, stats) in enumerate(sorted_movies)
    ]

    # Write to HDFS
    output_content = '\n'.join(output_lines)
    with hdfs_client.write(output_path) as writer:
        writer.write(output_content.encode('utf-8'))

    return sorted_movies