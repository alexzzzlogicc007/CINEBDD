import sqlite3
import time
import queries   # ton fichier queries.py

def measure_time(label, func, *args):
    start = time.perf_counter()
    result = func(*args)
    duration = (time.perf_counter() - start) * 1000
    print(f"{label:<30} {duration:.3f} ms")
    return duration

def main():
    conn = sqlite3.connect("data/imdb.db")

    print("=== Benchmark SANS index ===\n")

    times = {}

    times["Q1_filmographie"] = measure_time(
        "Q1 Filmographie",
        queries.query_actor_filmography, conn, "Tom Hanks"
    )

    times["Q2_top_n_films"] = measure_time(
        "Q2 Top N films",
        queries.query_top_n_films, conn, "Action", 2000, 2020, 10
    )

    times["Q3_multi_roles"] = measure_time(
        "Q3 Multi rôles",
        queries.query_multi_role_actors, conn
    )

    times["Q4_collab"] = measure_time(
        "Q4 Collaborations",
        queries.query_collaborations, conn, "Tom Hanks"
    )

    times["Q5_genres_populaires"] = measure_time(
        "Q5 Genres populaires",
        queries.query_popular_genres, conn
    )

    times["Q6_evolution"] = measure_time(
        "Q6 Evolution carrière",
        queries.query_career_evolution, conn, "Tom Hanks"
    )

    times["Q7_top3_genre"] = measure_time(
        "Q7 Top 3 genre",
        queries.query_top3_by_genre, conn
    )

    times["Q8_boost"] = measure_time(
        "Q8 Carrière propulsée",
        queries.query_career_boost, conn
    )

    times["Q9_libre"] = measure_time(
        "Q9 Requête libre",
        queries.query_custom, conn
    )

    print("\n=== Résultats FINALS (sans index) ===")
    for k, v in times.items():
        print(f"{k:25} : {v:.3f} ms")

    conn.close()

if __name__ == "__main__":
    main()
