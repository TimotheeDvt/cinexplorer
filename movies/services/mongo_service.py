from pymongo import MongoClient
from django.conf import settings

def get_mongo_client():
    client = MongoClient(
        host=settings.MONGODB_SETTINGS['host'],
        replicaset=settings.MONGODB_SETTINGS['replicaSet']
    )
    return client[settings.MONGODB_SETTINGS['db_name']]

def get_top_rated_movies(limit=10):
    """Récupère les films les mieux notés depuis MongoDB (documents structurés)."""
    db = get_mongo_client()

    movies = db.MOVIE_COMPLETE.find().sort("rating.average", -1).limit(limit)

    return list(movies)