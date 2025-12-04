import sqlite3
import csv
import os

DB_PATH = os.path.join("data", "imdb.db")
CSV_DIR = os.path.join("data", "csv")


def connect_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON;")
        print("Connected to SQLite database.")
        return conn
    except sqlite3.Error as e:
        print("Database connection error:", e)
        return None


def load_csv(filename):
    path = os.path.join(CSV_DIR, filename)
    rows = []

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    return rows


def insert_with_transaction(conn, table, rows, insert_query):
    cursor = conn.cursor()
    ok = 0
    fail = 0

    try:
        cursor.execute("BEGIN TRANSACTION;")

        for r in rows:
            try:
                cursor.execute(insert_query, r)
                ok += 1
            except Exception:
                fail += 1
                continue

        conn.commit()
        print(f"[{table}] Inserted: {ok} rows | Failed: {fail}")

    except Exception as e:
        conn.rollback()
        print(f"ERROR inserting in {table}:", e)


def main():
    conn = connect_db()
    if not conn:
        return

    print("\n=== IMPORT START ===\n")

    # 1️⃣ MOVIES
    insert_with_transaction(
        conn, "movies",
        load_csv("movies_clean.csv"),
        """INSERT INTO movies VALUES
        (:mid, :titleType, :primaryTitle, :originalTitle, :isAdult,
         :startYear, :endYear, :runtimeMinutes);"""
    )

    # 2️⃣ PERSONS
    persons = load_csv("persons_clean.csv")

    # 🔧 CORRECTION IMPORTANTE
    for p in persons:
        if "primaryName" in p:   # IMDB column
            p["name"] = p.pop("primaryName")

    insert_with_transaction(
        conn, "persons",
        persons,
        """INSERT INTO persons VALUES
        (:pid, :name, :birthYear, :deathYear);"""
    )

    # 3️⃣ GENRES
    insert_with_transaction(
        conn, "genres",
        load_csv("genres_clean.csv"),
        """INSERT INTO genres VALUES (:mid, :genre);"""
    )

    # 4️⃣ RATINGS
    insert_with_transaction(
        conn, "ratings",
        load_csv("ratings_clean.csv"),
        """INSERT INTO ratings VALUES (:mid, :averageRating, :numVotes);"""
    )

    # 5️⃣ PRINCIPALS
    principals = load_csv("principals_clean.csv")

    for r in principals:
        if "nconst" in r:
            r["pid"] = r.pop("nconst")
        if "tconst" in r:
            r["mid"] = r.pop("tconst")

    insert_with_transaction(
        conn, "principals",
        principals,
        """INSERT INTO principals VALUES
        (:mid, :pid, :ordering, :category, :job);"""
    )

    # 6️⃣ KNOWN FOR MOVIES
    known = load_csv("knownformovies_clean.csv")

    for r in known:
        if "nconst" in r:
            r["pid"] = r.pop("nconst")
        if "tconst" in r:
            r["mid"] = r.pop("tconst")

    insert_with_transaction(
        conn, "known_for_movies",
        known,
        """INSERT INTO known_for_movies VALUES (:pid, :mid);"""
    )

# 7️⃣ PROFESSIONS
    professions = load_csv("professions_clean.csv")

# Correction du nom de colonne
    for r in professions:
     if "jobName" in r:
        r["profession"] = r.pop("jobName")

    insert_with_transaction(
    conn, "professions",
    professions,
    """INSERT INTO professions VALUES (:pid, :profession);"""
)

    # 8️⃣ WRITERS
    writers = load_csv("writers_clean.csv")

    for r in writers:
        if "nconst" in r:
            r["pid"] = r.pop("nconst")
        if "tconst" in r:
            r["mid"] = r.pop("tconst")

    insert_with_transaction(
        conn, "writers",
        writers,
        """INSERT INTO writers VALUES (:mid, :pid);"""
    )

    # 9️⃣ DIRECTORS
    directors = load_csv("directors_clean.csv")

    for r in directors:
        if "nconst" in r:
            r["pid"] = r.pop("nconst")
        if "tconst" in r:
            r["mid"] = r.pop("tconst")

    insert_with_transaction(
        conn, "directors",
        directors,
        """INSERT INTO directors VALUES (:mid, :pid);"""
    )


    conn.close()
    print("\n=== IMPORT FINISHED ===\n")


if __name__ == "__main__":
    main()
