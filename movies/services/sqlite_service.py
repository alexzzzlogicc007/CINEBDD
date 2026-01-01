import sqlite3
from pathlib import Path


DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "imdb.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


def count_movies():
    """
    Retourne le nombre de films dans SQLite.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM movies")
    result = cursor.fetchone()[0]
    conn.close()
    return result


def count_persons():
    """
    Retourne le nombre de personnes dans SQLite.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM persons")
    result = cursor.fetchone()[0]
    conn.close()
    return result
