import sqlite3
import os

DB_PATH = os.path.join("data", "imdb.db")

def create_connection():
    """Create a database connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        print("Connected to SQLite database.")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    return conn


def create_tables(conn):
    """Create all tables according to the relational schema (3NF)."""
    cursor = conn.cursor()

    # Enable foreign key enforcement
    cursor.execute("PRAGMA foreign_keys = ON;")

    # MOVIES
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            mid TEXT PRIMARY KEY,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER
        );
    """)

    # PERSONS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS persons (
            pid TEXT PRIMARY KEY,
            name TEXT,
            birthYear INTEGER,
            deathYear INTEGER
        );
    """)

    # GENRES (PK = mid, genre)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS genres (
            mid TEXT,
            genre TEXT,
            PRIMARY KEY (mid, genre),
            FOREIGN KEY (mid) REFERENCES movies(mid)
        );
    """)

    # RATINGS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            mid TEXT PRIMARY KEY,
            averageRating REAL,
            numVotes INTEGER,
            FOREIGN KEY (mid) REFERENCES movies(mid)
        );
    """)

    # PRINCIPALS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS principals (
            mid TEXT,
            pid TEXT,
            ordering INTEGER,
            category TEXT,
            job TEXT,
            PRIMARY KEY (mid, pid),
            FOREIGN KEY (mid) REFERENCES movies(mid),
            FOREIGN KEY (pid) REFERENCES persons(pid)
        );
    """)

    # KNOWN FOR MOVIES
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS known_for_movies (
            pid TEXT,
            mid TEXT,
            PRIMARY KEY (pid, mid),
            FOREIGN KEY (pid) REFERENCES persons(pid),
            FOREIGN KEY (mid) REFERENCES movies(mid)
        );
    """)

    # PROFESSIONS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS professions (
            pid TEXT,
            profession TEXT,
            PRIMARY KEY (pid, profession),
            FOREIGN KEY (pid) REFERENCES persons(pid)
        );
    """)

    # WRITERS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS writers (
            mid TEXT,
            pid TEXT,
            PRIMARY KEY (mid, pid),
            FOREIGN KEY (mid) REFERENCES movies(mid),
            FOREIGN KEY (pid) REFERENCES persons(pid)
        );
    """)

    # DIRECTORS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS directors (
            mid TEXT,
            pid TEXT,
            PRIMARY KEY (mid, pid),
            FOREIGN KEY (mid) REFERENCES movies(mid),
            FOREIGN KEY (pid) REFERENCES persons(pid)
        );
    """)

    conn.commit()
    print("All tables created successfully.")


def main():
    conn = create_connection()
    if conn:
        create_tables(conn)
        conn.close()


if __name__ == "__main__":
    main()
