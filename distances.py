"""
``from_point`` is a list of the format `[longitude, latitude]`

``to_points`` is a list of lists of the format `[[longitude1, latitude1], [longitude2, latitude2],...]`
"""

from geohash import encode as geohash
import redis
import requests

r = redis.Redis(host='127.0.0.1', port=6379, db=1)


def get_distances(from_point: list[float, float], to_points: list[list[float, float]]):
    """
    Try to get cached distances between the given points from Redis. For points
    for which the distances are not available, get then from GraphHopper (which
    also caches them in Redis).
    """
    from_point_geohash = geohash(from_point[1], from_point[0], 6)
    to_point_geohashes = [
        geohash(to_point[1], to_point[0], 6)
        for to_point in to_points
    ]
    distance_redis_keys = [
        get_distance_redis_key(from_point_geohash, to_point_geohash)
        for to_point_geohash in to_point_geohashes
    ]
    cached_distances = r.mget(distance_redis_keys)
    print('Got distances from Redis')
    print(cached_distances)

    to_points_not_found = []
    for i in range(len(cached_distances)):
        try:
            cached_distances[i] = float(cached_distances[i])
        except (ValueError, TypeError):
            cached_distances[i] = None
            to_points_not_found.append(to_points[i])
    print(f'{len(to_points_not_found)} distances not found in Redis')

    fresh_distances = get_distances_from_graphhopper(from_point, to_points_not_found)

    distances = cached_distances
    i = 0
    j = 0
    while i < len(distances) and j < len(fresh_distances):
        if distances[i] is None:
            distances[i] = fresh_distances[j]
            j += 1
        i += 1

    return distances


def get_distances_from_graphhopper(from_point: list[float, float], to_points: list[list[float, float]]):
    """
    Get distances between the given points from GraphHopper's distance matrix
    API. Then cache those distances in Redis.
    """
    url = 'https://graphhopper.com/api/1/matrix?key=650066a4-4835-4d34-8066-fc79d2aceab4'

    payload = {
        'from_points': [from_point],
        'to_points': to_points,
        'out_arrays': [
            'distances'
        ],
        'fail_fast': False,
        'vehicle': 'car'
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    distances = response.json()['distances'][0]
    print('Got distances from GraphHopper')
    print(distances)
    save_distances_in_redis(from_point, to_points, distances)

    return distances


def get_distance_redis_key(geohash1, geohash2):
    if geohash2 < geohash1:
        geohash1, geohash2 = geohash2, geohash1
    return f'dist_{geohash1}_{geohash2}'


def save_distances_in_redis(from_point: list[float, float], to_points: list[list[float, float]], distances: list[float]):
    from_point_geohash = geohash(from_point[1], from_point[0], 6)
    to_point_geohashes = [
        geohash(to_point[1], to_point[0], 6)
        for to_point in to_points
    ]
    distance_redis_keys = [
        get_distance_redis_key(from_point_geohash, to_point_geohash)
        for to_point_geohash in to_point_geohashes
    ]
    for i in range(len(distance_redis_keys)):
        if distances[i] is not None:
            r.set(distance_redis_keys[i], distances[i], 180 * 24 * 60 * 60)     # cache for 180 days
            print(f'Saved distance in Redis')
        else:
            print(f'Skipped saving invalid distance `{distances[i]}` for `{distance_redis_keys[i]}` key in Redis')


if __name__ == '__main__':
    from_point = [77.2779, 28.5250701]
    to_points = [
        [77.4443349,28.6440302],
        [77.809263,28.7960826],
        [78.1625339,28.6953879],
        [78.5085326,28.4668932],
        [78.7895876,28.2120983],
        [78.884287,27.8786497],
        [79.0212586,27.5219363],
        [78.9714207,27.1420933],
        [79.2236661,26.8060249],
        [79.5228177,26.4699747],
        [80.2571777,26.4131853],
        [80.8962784,26.3133233],
    ]
    distances = get_distances(from_point, to_points)
