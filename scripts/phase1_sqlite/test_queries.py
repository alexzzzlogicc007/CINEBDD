import sqlite3
from queries import *

conn = sqlite3.connect("data/imdb.db")

print("\n--- Filmographie : Tom Hanks ---")
print(query_actor_filmography(conn, "Tom Hanks")[:10])

print("\n--- Top films Action 2000-2020 ---")
print(query_top_n_films(conn, "Action", 2000, 2020, 5))

print("\n--- Acteurs multi-rôles ---")
print(query_multi_role_actors(conn)[:10])

print("\n--- Collaborations avec Tom Hanks ---")
print(query_collaborations(conn, "Tom Hanks")[:10])

print("\n--- Genres populaires ---")
print(query_popular_genres(conn)[:10])

print("\n--- Évolution carrière Tom Hanks ---")
print(query_career_evolution(conn, "Tom Hanks"))

print("\n--- Top 3 films par genre ---")
print(query_top3_by_genre(conn)[:10])

print("\n--- Carrière propulsée ---")
print(query_career_boost(conn)[:10])

print("\n--- Requête libre ---")
print(query_custom(conn)[:10])
