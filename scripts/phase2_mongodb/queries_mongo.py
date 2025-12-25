from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["imdb"]

MAX_TIME_MS = 15000  # 15 secondes


###############################################
# 1. Filmographie d’un acteur
###############################################
def mongo_actor_filmography(actor_name):
    pipeline = [
        {"$match": {"name": {"$regex": actor_name, "$options": "i"}}},
        {"$lookup": {
            "from": "principals",
            "localField": "pid",
            "foreignField": "pid",
            "as": "roles"
        }},
        {"$unwind": "$roles"},
        {"$lookup": {
            "from": "movies",
            "localField": "roles.mid",
            "foreignField": "mid",
            "as": "movie"
        }},
        {"$unwind": "$movie"},
        {"$project": {
            "_id": 0,
            "title": "$movie.primaryTitle",
            "year": "$movie.startYear",
            "category": "$roles.category",
            "job": "$roles.job"
        }},
        {"$sort": {"year": -1}}
    ]
    return list(db.persons.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))


###############################################
# 2. Top N films d’un genre sur une période
###############################################
def mongo_top_n_films(genre, start, end, n):
    pipeline = [
        {"$match": {"genre": genre}},
        {"$lookup": {
            "from": "movies",
            "localField": "mid",
            "foreignField": "mid",
            "as": "movie"
        }},
        {"$unwind": "$movie"},
        {"$lookup": {
            "from": "ratings",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating"
        }},
        {"$unwind": "$rating"},
        {"$match": {"movie.startYear": {"$gte": start, "$lte": end}}},
        {"$project": {
            "_id": 0,
            "title": "$movie.primaryTitle",
            "year": "$movie.startYear",
            "rating": "$rating.averageRating"
        }},
        {"$sort": {"rating": -1}},
        {"$limit": n}
    ]
    return list(db.genres.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))


###############################################
# 3. Acteurs ayant joué plusieurs rôles
###############################################
def mongo_multi_role_actors():
    pipeline = [
        {"$group": {
            "_id": {"mid": "$mid", "pid": "$pid"},
            "roles": {
                "$addToSet": {
                    "$concat": ["$category", "-", {"$ifNull": ["$job", ""]}]
                }
            }
        }},
        {"$project": {
            "mid": "$_id.mid",
            "pid": "$_id.pid",
            "nb_roles": {"$size": "$roles"}
        }},
        {"$match": {"nb_roles": {"$gt": 1}}},
        {"$lookup": {
            "from": "persons",
            "localField": "pid",
            "foreignField": "pid",
            "as": "person"
        }},
        {"$unwind": "$person"},
        {"$lookup": {
            "from": "movies",
            "localField": "mid",
            "foreignField": "mid",
            "as": "movie"
        }},
        {"$unwind": "$movie"},
        {"$project": {
            "_id": 0,
            "name": "$person.name",
            "title": "$movie.primaryTitle",
            "nb_roles": 1
        }},
        {"$sort": {"nb_roles": -1}}
    ]
    return list(db.principals.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))


###############################################
# 4. Collaborations réalisateur / acteur
###############################################
def mongo_collaborations(actor_name):
    pipeline = [
        {"$match": {"name": {"$regex": actor_name, "$options": "i"}}},
        {"$lookup": {
            "from": "principals",
            "localField": "pid",
            "foreignField": "pid",
            "as": "actor_movies"
        }},
        {"$unwind": "$actor_movies"},
        {"$lookup": {
            "from": "directors",
            "localField": "actor_movies.mid",
            "foreignField": "mid",
            "as": "directors"
        }},
        {"$unwind": "$directors"},
        {"$lookup": {
            "from": "persons",
            "localField": "directors.pid",
            "foreignField": "pid",
            "as": "director"
        }},
        {"$unwind": "$director"},
        {"$group": {
            "_id": "$director.name",
            "collaborations": {"$sum": 1}
        }},
        {"$sort": {"collaborations": -1}}
    ]
    return list(db.persons.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))


###############################################
# 5. Genres populaires
###############################################
def mongo_popular_genres():
    pipeline = [
        {"$lookup": {
            "from": "ratings",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating"
        }},
        {"$unwind": "$rating"},
        {"$group": {
            "_id": "$genre",
            "nb_films": {"$sum": 1},
            "note_moyenne": {"$avg": "$rating.averageRating"}
        }},
        {"$match": {
            "note_moyenne": {"$gt": 7.0},
            "nb_films": {"$gt": 50}
        }},
        {"$project": {
            "_id": 0,
            "genre": "$_id",
            "nb_films": 1,
            "note_moyenne": 1
        }},
        {"$sort": {"note_moyenne": -1}}
    ]
    return list(db.genres.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))


###############################################
# 6. Évolution de carrière par décennie
###############################################
def mongo_career_evolution(actor_name):
    pipeline = [
        {"$match": {"name": {"$regex": actor_name, "$options": "i"}}},
        {"$lookup": {
            "from": "principals",
            "localField": "pid",
            "foreignField": "pid",
            "as": "roles"
        }},
        {"$unwind": "$roles"},
        {"$lookup": {
            "from": "movies",
            "localField": "roles.mid",
            "foreignField": "mid",
            "as": "movie"
        }},
        {"$unwind": "$movie"},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie.mid",
            "foreignField": "mid",
            "as": "rating"
        }},
        {"$unwind": {"path": "$rating", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "decade": {
                "$multiply": [
                    {"$floor": {"$divide": ["$movie.startYear", 10]}},
                    10
                ]
            },
            "rating": "$rating.averageRating"
        }},
        {"$group": {
            "_id": "$decade",
            "nb_films": {"$sum": 1},
            "note_moyenne": {"$avg": "$rating"}
        }},
        {"$project": {
            "_id": 0,
            "decade": "$_id",
            "nb_films": 1,
            "note_moyenne": 1
        }},
        {"$sort": {"decade": 1}}
    ]
    return list(db.persons.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))


###############################################
# 7. Top 3 films par genre
###############################################
def mongo_top3_by_genre():
    pipeline = [
        {"$lookup": {
            "from": "movies",
            "localField": "mid",
            "foreignField": "mid",
            "as": "movie"
        }},
        {"$unwind": "$movie"},
        {"$lookup": {
            "from": "ratings",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating"
        }},
        {"$unwind": "$rating"},
        {"$sort": {"genre": 1, "rating.averageRating": -1}},
        {"$group": {
            "_id": "$genre",
            "films": {"$push": {
                "title": "$movie.primaryTitle",
                "rating": "$rating.averageRating"
            }}
        }},
        {"$unwind": {"path": "$films", "includeArrayIndex": "rank"}},
        {"$project": {
            "_id": 0,
            "genre": "$_id",
            "title": "$films.title",
            "rating": "$films.rating",
            "rank": {"$add": ["$rank", 1]}
        }},
        {"$match": {"rank": {"$lte": 3}}},
        {"$sort": {"genre": 1, "rank": 1}}
    ]
    return list(db.genres.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))


###############################################
# 8. Carrière propulsée
###############################################
def mongo_career_boost():
    pipeline = [
        {"$lookup": {
            "from": "ratings",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating"
        }},
        {"$unwind": "$rating"},
        {"$group": {
            "_id": "$pid",
            "avant": {"$sum": {"$cond": [{"$lt": ["$rating.numVotes", 200000]}, 1, 0]}},
            "apres": {"$sum": {"$cond": [{"$gte": ["$rating.numVotes", 200000]}, 1, 0]}}
        }},
        {"$match": {"apres": {"$gt": 0}}},
        {"$lookup": {
            "from": "persons",
            "localField": "_id",
            "foreignField": "pid",
            "as": "person"
        }},
        {"$unwind": "$person"},
        {"$project": {
            "_id": 0,
            "name": "$person.name",
            "avant": 1,
            "apres": 1
        }},
        {"$sort": {"apres": -1}}
    ]
    return list(db.principals.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))


###############################################
# 9. Requête libre
###############################################
def mongo_custom():
    pipeline = [
        {"$lookup": {
            "from": "directors",
            "localField": "mid",
            "foreignField": "mid",
            "as": "directors"
        }},
        {"$lookup": {
            "from": "writers",
            "localField": "mid",
            "foreignField": "mid",
            "as": "writers"
        }},
        {"$lookup": {
            "from": "principals",
            "localField": "mid",
            "foreignField": "mid",
            "as": "cast"
        }},
        {"$project": {
            "primaryTitle": 1,
            "director_pids": "$directors.pid",
            "writer_pids": "$writers.pid",
            "cast_pids": "$cast.pid"
        }},
        {"$match": {"$expr": {"$or": [
            {"$gt": [{"$size": {"$setIntersection": ["$director_pids", "$cast_pids"]}}, 0]},
            {"$gt": [{"$size": {"$setIntersection": ["$writer_pids", "$cast_pids"]}}, 0]}
        ]}}},
        {"$lookup": {
            "from": "persons",
            "localField": "director_pids",
            "foreignField": "pid",
            "as": "directors"
        }},
        {"$lookup": {
            "from": "persons",
            "localField": "writer_pids",
            "foreignField": "pid",
            "as": "writers"
        }},
        {"$project": {
            "_id": 0,
            "title": "$primaryTitle",
            "director": {"$arrayElemAt": ["$directors.name", 0]},
            "writer": {"$arrayElemAt": ["$writers.name", 0]}
        }}
    ]
    return list(db.movies.aggregate(pipeline, maxTimeMS=MAX_TIME_MS))
