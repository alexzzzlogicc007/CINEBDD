from pymongo import MongoClient
import time


# Connexion MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["imdb"]

#  Création de la collection movies_complete (DENORMALISÉE)
def create_movies_complete():
    print("\n Création de la collection movies_complete (échantillon limité)...")

    start = time.perf_counter()

    # Nettoyage préalable
    db.movies_complete.drop()

    pipeline = [

        # LIMITE EXPLICITE (ESSENTIELLE)
        # Échantillon représentatif, pas toute IMDb
        {"$limit": 500},

        # ----------------------------
        # Genres
        # ----------------------------
        {
            "$lookup": {
                "from": "genres",
                "localField": "mid",
                "foreignField": "mid",
                "as": "genres"
            }
        },

        # ----------------------------
        # Ratings
        # ----------------------------
        {
            "$lookup": {
                "from": "ratings",
                "localField": "mid",
                "foreignField": "mid",
                "as": "rating"
            }
        },

        # ----------------------------
        # Directors
        # ----------------------------
        {
            "$lookup": {
                "from": "directors",
                "localField": "mid",
                "foreignField": "mid",
                "as": "directors"
            }
        },
        {
            "$lookup": {
                "from": "persons",
                "localField": "directors.pid",
                "foreignField": "pid",
                "as": "director_persons"
            }
        },

        # ----------------------------
        # Cast
        # ----------------------------
        {
            "$lookup": {
                "from": "principals",
                "localField": "mid",
                "foreignField": "mid",
                "as": "cast"
            }
        },
        {
            "$lookup": {
                "from": "persons",
                "localField": "cast.pid",
                "foreignField": "pid",
                "as": "cast_persons"
            }
        },

        # ----------------------------
        # Writers
        # ----------------------------
        {
            "$lookup": {
                "from": "writers",
                "localField": "mid",
                "foreignField": "mid",
                "as": "writers"
            }
        },
        {
            "$lookup": {
                "from": "persons",
                "localField": "writers.pid",
                "foreignField": "pid",
                "as": "writer_persons"
            }
        },

        # ----------------------------
        # Projection finale (DOCUMENT STRUCTURÉ)
        # ----------------------------
        {
            "$project": {
                "_id": "$mid",
                "title": "$primaryTitle",
                "year": "$startYear",
                "runtime": "$runtimeMinutes",

                "genres": "$genres.genre",

                "rating": {
                    "average": {"$arrayElemAt": ["$rating.averageRating", 0]},
                    "votes": {"$arrayElemAt": ["$rating.numVotes", 0]}
                },

                "directors": {
                    "$map": {
                        "input": "$director_persons",
                        "as": "d",
                        "in": {
                            "person_id": "$$d.pid",
                            "name": "$$d.name"
                        }
                    }
                },

                "cast": {
                    "$map": {
                        "input": "$cast",
                        "as": "c",
                        "in": {
                            "person_id": "$$c.pid",
                            "ordering": "$$c.ordering",
                            "characters": "$$c.characters"
                        }
                    }
                },

                "writers": {
                    "$map": {
                        "input": "$writer_persons",
                        "as": "w",
                        "in": {
                            "person_id": "$$w.pid",
                            "name": "$$w.name"
                        }
                    }
                }
            }
        },

        # ----------------------------
        # Création de la collection finale
        # ----------------------------
        {
            "$out": "movies_complete"
        }
    ]

    db.movies.aggregate(pipeline, allowDiskUse=True)

    elapsed = time.perf_counter() - start
    count = db.movies_complete.count_documents({})

    print(f"movies_complete créée")
    print(f"   → Documents : {count}")
    print(f"   → Temps total : {elapsed:.2f} secondes")


# ============================================================
# Accès à un film — modèle PLAT (N requêtes)
# ============================================================
def get_movie_flat(mid):
    start = time.perf_counter()

    movie = db.movies.find_one({"mid": mid})
    genres = list(db.genres.find({"mid": mid}))
    rating = db.ratings.find_one({"mid": mid})
    cast = list(db.principals.find({"mid": mid}))
    directors = list(db.directors.find({"mid": mid}))
    writers = list(db.writers.find({"mid": mid}))

    elapsed = time.perf_counter() - start
    return elapsed, {
        "movie": movie,
        "genres": genres,
        "rating": rating,
        "cast": cast,
        "directors": directors,
        "writers": writers
    }


# ============================================================
# Accès à un film — modèle STRUCTURÉ (1 requête)
# ============================================================
def get_movie_structured(mid):
    start = time.perf_counter()
    movie = db.movies_complete.find_one({"_id": mid})
    elapsed = time.perf_counter() - start
    return elapsed, movie


# ============================================================
#  Comparaison taille de stockage
# ============================================================
def compare_storage_size():
    flat_size = db.command("collstats", "movies")["size"]
    structured_size = db.command("collstats", "movies_complete")["size"]

    print("\n Taille des collections :")
    print(f"movies (plat)          : {flat_size / 1024 / 1024:.2f} MB")
    print(f"movies_complete        : {structured_size / 1024 / 1024:.2f} MB")


# ============================================================
# Point d’entrée principal
# ============================================================
if __name__ == "__main__":
    MID_TEST = "tt0111161"  # Shawshank Redemption

    # 1. Création collection structurée
    create_movies_complete()

    # 2. Comparaison temps d’accès
    print("\n #Comparaison du temps d’accès à un film complet")

    flat_time, _ = get_movie_flat(MID_TEST)
    structured_time, _ = get_movie_structured(MID_TEST)

    print(f"Modèle plat (N requêtes)     : {flat_time * 1000:.2f} ms")
    print(f"Modèle structuré (1 requête): {structured_time * 1000:.2f} ms")

    # 3. Comparaison taille
    compare_storage_size()
