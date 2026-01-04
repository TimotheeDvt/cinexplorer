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

def get_movie_detail(movie_id):
    """Récupère le document complet depuis MongoDB."""
    db = get_mongo_client()
    print(db.MOVIE_COMPLETE.find_one({"_id": movie_id}))
    return db.MOVIE_COMPLETE.find_one({"_id": movie_id})

def get_similar_movies(movie, limit=4):
    """Trouve des films similaires basés sur le genre ou le réalisateur si dispo."""
    if not movie:
        return []

    db = get_mongo_client()
    or_filters = []

    # On n'ajoute les filtres que si les listes ne sont pas vides
    genres = movie.get('genres', [])
    if genres:
        or_filters.append({"genres": {"$in": genres}})

    directors = [d.get('name') for d in movie.get('directors', []) if d.get('name')]
    if directors:
        or_filters.append({"directors.name": {"$in": directors}})

    if not or_filters:
        return []

    query = {
        "_id": {"$ne": movie["_id"]},
        "$or": or_filters
    }

    return list(db.MOVIE_COMPLETE.find(query).limit(limit))

def get_rating_distribution():
    """Récupère la distribution des notes via MongoDB (Histogramme)."""
    db = get_mongo_client()
    pipeline = [
        {"$match": {"rating.average": {"$exists": True}}},
        {"$group": {
            "_id": {"$floor": "$rating.average"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    results = list(db.MOVIE_COMPLETE.aggregate(pipeline))
    return [{'label': r['_id'], 'value': r['count']} for r in results]