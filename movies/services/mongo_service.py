from pymongo import MongoClient
from django.conf import settings

# Connexion au Replica Set MongoDB
_client = MongoClient(settings.MONGO_URI)
_db = _client[settings.MONGO_DB_NAME]


def count_movies():
    """
    Retourne le nombre total de films dans MongoDB.
    """
    return _db.movies.count_documents({})


def count_persons():
    """
    Retourne le nombre total de personnes dans MongoDB.
    """
    return _db.persons.count_documents({})


def top_genres(limit=5):
    """
    Retourne les genres les plus fréquents.
    """
    pipeline = [
        {"$group": {"_id": "$genre", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit}
    ]
    return list(_db.genres.aggregate(pipeline))
