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


def get_movie_by_id(movie_id: str):
    """
    Retourne un film complet depuis la collection movies_complete
    (1 requête MongoDB).
    """
    return _db.movies_complete.find_one({"_id": movie_id})

    # Si tu utilises _id à la place :
    # return _db.movies_complete.find_one({"_id": movie_id})


def get_similar_movies(movie: dict, limit=6):
    """
    Retourne des films similaires (même genre ou même réalisateur).
    """
    genres = movie.get("genres", [])
    directors = [d["name"] for d in movie.get("directors", [])]

    query = {
        "$and": [
            {"_id": {"$ne": movie.get("_id")}},
            {"$or": [
                {"genres": {"$in": genres}},
                {"directors.name": {"$in": directors}},
            ]}
        ]
    }

    return list(_db.movies_complete.find(query).limit(limit))