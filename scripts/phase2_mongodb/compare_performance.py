import time
import sqlite3
from tabulate import tabulate

# ============================
# Import requêtes SQLite
# ============================
from scripts.phase1_sqlite.queries import (
    query_actor_filmography,
    query_top_n_films,
    query_multi_role_actors,
    query_collaborations,
    query_popular_genres,
    query_career_evolution,
    query_top3_by_genre,
    query_career_boost,
    query_custom
)

# ============================
# Import requêtes MongoDB
# ============================
from scripts.phase2_mongodb.queries_mongo import (
    mongo_actor_filmography,
    mongo_top_n_films,
    mongo_multi_role_actors,
    mongo_collaborations,
    mongo_popular_genres,
    mongo_career_evolution,
    mongo_top3_by_genre,
    mongo_career_boost,
    mongo_custom
)

# ============================
# Connexion SQLite
# ============================
SQLITE_DB_PATH = "data/imdb.db"
conn = sqlite3.connect(SQLITE_DB_PATH)

# ============================
# Fonction de benchmark
# ============================
from pymongo.errors import ExecutionTimeout

def benchmark_query(label, sqlite_func, mongo_func):
    # SQLite
    start_sqlite = time.perf_counter()
    sqlite_func()
    sqlite_time = round((time.perf_counter() - start_sqlite) * 1000, 2)

    # MongoDB
    start_mongo = time.perf_counter()
    try:
        mongo_func()
        mongo_time = round((time.perf_counter() - start_mongo) * 1000, 2)

    except ExecutionTimeout:
        mongo_time = "timeout (>15s)"

    except KeyboardInterrupt:
        mongo_time = "interrompu"

    return {
        "Requête": label,
        "SQLite (ms)": sqlite_time,
        "MongoDB (ms)": mongo_time
    }

# ============================
# Lancement des benchmarks
# ============================
def run_all_benchmarks():
    results = []

    results.append(benchmark_query(
        "1. Filmographie d’un acteur",
        lambda: query_actor_filmography(conn, "Tom Hanks"),
        lambda: mongo_actor_filmography("Tom Hanks")
    ))

    results.append(benchmark_query(
        "2. Top N films par genre",
        lambda: query_top_n_films(conn, "Drama", 1990, 2010, 10),
        lambda: mongo_top_n_films("Drama", 1990, 2010, 10)
    ))

    results.append(benchmark_query(
        "3. Acteurs multi-rôles",
        lambda: query_multi_role_actors(conn),
        lambda: mongo_multi_role_actors()
    ))

    results.append(benchmark_query(
        "4. Collaborations réalisateur / acteur",
        lambda: query_collaborations(conn, "Tom Hanks"),
        lambda: mongo_collaborations("Tom Hanks")
    ))

    results.append(benchmark_query(
        "5. Genres populaires",
        lambda: query_popular_genres(conn),
        lambda: mongo_popular_genres()
    ))

    results.append(benchmark_query(
        "6. Évolution de carrière",
        lambda: query_career_evolution(conn, "Tom Hanks"),
        lambda: mongo_career_evolution("Tom Hanks")
    ))

    results.append(benchmark_query(
        "7. Top 3 films par genre",
        lambda: query_top3_by_genre(conn),
        lambda: mongo_top3_by_genre()
    ))

    results.append(benchmark_query(
        "8. Carrière propulsée",
        lambda: query_career_boost(conn),
        lambda: mongo_career_boost()
    ))

    results.append(benchmark_query(
        "9. Requête libre",
        lambda: query_custom(conn),
        lambda: mongo_custom()
    ))

    return results

# ============================
# Point d’entrée
# ============================
if __name__ == "__main__":
    print("\n⏱️ Benchmark des requêtes SQLite vs MongoDB\n")

    benchmark_results = run_all_benchmarks()

    print(tabulate(
        benchmark_results,
        headers="keys",
        tablefmt="grid"
    ))

    conn.close()
