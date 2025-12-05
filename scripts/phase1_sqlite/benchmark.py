import sqlite3
import time
from queries import *

def benchmark(func, conn, *args):
    t0 = time.perf_counter()
    func(conn, *args)
    return (time.perf_counter() - t0) * 1000  # ms

# ----------------------------------------
# 1) Connexion + création des index
# ----------------------------------------

conn = sqlite3.connect("data/imdb.db")
cur = conn.cursor()

print("=== Création des index ===")

indexes = [
    "CREATE INDEX IF NOT EXISTS idx_principals_pid ON principals(pid);",
    "CREATE INDEX IF NOT EXISTS idx_principals_mid ON principals(mid);",
    "CREATE INDEX IF NOT EXISTS idx_persons_name ON persons(name);",
    "CREATE INDEX IF NOT EXISTS idx_genres_mid ON genres(mid);",
    "CREATE INDEX IF NOT EXISTS idx_ratings_mid ON ratings(mid);",
    "CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(startYear);"
]

for sql in indexes:
    cur.execute(sql)

conn.commit()
print("Index créés.\n")

# ----------------------------------------
# 2) Benchmark AVEC index
# ----------------------------------------

print("=== Benchmark AVEC INDEX ===")

results = {}

results["Q1"] = benchmark(query_actor_filmography, conn, "Tom Hanks")
print(f"Q1 Filmographie : {results['Q1']:.2f} ms")

results["Q2"] = benchmark(query_top_n_films, conn, "Action", 2000, 2020, 10)
print(f"Q2 Top N films : {results['Q2']:.2f} ms")

results["Q3"] = benchmark(query_multi_role_actors, conn)
print(f"Q3 Multi-rôles : {results['Q3']:.2f} ms")

results["Q4"] = benchmark(query_collaborations, conn, "Tom Hanks")
print(f"Q4 Collaborations : {results['Q4']:.2f} ms")

results["Q5"] = benchmark(query_popular_genres, conn)
print(f"Q5 Genres populaires : {results['Q5']:.2f} ms")

results["Q6"] = benchmark(query_career_evolution, conn, "Tom Hanks")
print(f"Q6 Évolution carrière : {results['Q6']:.2f} ms")

results["Q7"] = benchmark(query_top3_by_genre, conn)
print(f"Q7 Top 3 par genre : {results['Q7']:.2f} ms")

results["Q8"] = benchmark(query_career_boost, conn)
print(f"Q8 Carrière propulsée : {results['Q8']:.2f} ms")

results["Q9"] = benchmark(query_custom, conn)
print(f"Q9 Requête libre : {results['Q9']:.2f} ms")

conn.close()
