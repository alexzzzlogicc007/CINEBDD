from pymongo import MongoClient
import time


# ============================================================
# Connexion MongoDB
# ============================================================
client = MongoClient("mongodb://localhost:27017/")
db = client["imdb"]


# ============================================================
# Création de la collection movies_complete (DENORMALISÉE)
# ============================================================
def create_movies_complete():
    print("\nCréation de la collection movies_complete (TOUS les films)...")

    start = time.perf_counter()

    # Nettoyage préalable
    db.movies_complete.drop()

    pipeline = [

        # ----------------------------------------------------
        # Garder uniquement les vrais films
        # ----------------------------------------------------
        {
            "$match": {
                "titleType": "movie"
            }
        },

        # ----------------------------------------------------
        # Genres
        # ----------------------------------------------------
        {
            "$lookup": {
                "from": "genres",
                "localField": "mid",
                "foreignField": "mid",
                "as": "genres"
            }
        },

        # ----------------------------------------------------
        # Ratings
        # ----------------------------------------------------
        {
            "$lookup": {
                "from": "ratings",
                "localField": "mid",
                "foreignField": "mid",
                "as": "rating_arr"
            }
        },

        # ----------------------------------------------------
        # Directors
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # Cast
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # Writers
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # Projection finale (DOCUMENT STRUCTURÉ CONFORME)
        # ----------------------------------------------------
        {
            "$project": {
                "_id": "$mid",
                "title": "$primaryTitle",
                "year": "$startYear",
                "runtime": "$runtimeMinutes",

                "genres": "$genres.genre",

                "rating": {
                    "average": { "$arrayElemAt": ["$rating_arr.averageRating", 0] },
                    "votes": { "$arrayElemAt": ["$rating_arr.numVotes", 0] }
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
                            "characters": "$$c.characters",
                            "name": {
                                "$arrayElemAt": [
                                    {
                                        "$map": {
                                            "input": {
                                                "$filter": {
                                                    "input": "$cast_persons",
                                                    "as": "p",
                                                    "cond": {
                                                        "$eq": ["$$p.pid", "$$c.pid"]
                                                    }
                                                }
                                            },
                                            "as": "cp",
                                            "in": "$$cp.name"
                                        }
                                    },
                                    0
                                ]
                            }
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

        # ----------------------------------------------------
        # Création de la collection finale
        # ----------------------------------------------------
        {
            "$out": "movies_complete"
        }
    ]

    db.movies.aggregate(pipeline, allowDiskUse=True)

    elapsed = time.perf_counter() - start
    count = db.movies_complete.count_documents({})

    print("movies_complete créée")
    print(f" → Documents : {count}")
    print(f" → Temps total : {elapsed:.2f} secondes")


# ============================================================
# Point d’entrée du script
# ============================================================
if __name__ == "__main__":
    create_movies_complete()
