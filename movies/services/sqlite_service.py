import sqlite3
from django.conf import settings

DB_PATH = settings.DATABASES["default"]["NAME"]


def get_connection():
    return sqlite3.connect(DB_PATH)


def execute_query(sql, params=()):
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(sql, params).fetchall()

def count_movies():
    with get_connection() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM movies"
        ).fetchone()[0]


def count_actors():
    with get_connection() as conn:
        return conn.execute("""
            SELECT COUNT(DISTINCT pid)
            FROM principals
            WHERE category IN ('actor','actress')
        """).fetchone()[0]


def count_directors():
    with get_connection() as conn:
        return conn.execute("""
            SELECT COUNT(DISTINCT pid)
            FROM principals
            WHERE category = 'director'
        """).fetchone()[0]

def get_top_movies(limit=10):
    with get_connection() as conn:
        return conn.execute("""
            SELECT m.mid,
                   m.primaryTitle,
                   m.startYear,
                   r.averageRating
            FROM movies m
            JOIN ratings r ON m.mid = r.mid
            WHERE r.averageRating IS NOT NULL
            ORDER BY r.averageRating DESC
            LIMIT ?
        """, (limit,)).fetchall()


def get_random_movies(limit=6):
    with get_connection() as conn:
        return conn.execute("""
            SELECT mid,
                   primaryTitle,
                   startYear
            FROM movies
            ORDER BY RANDOM()
            LIMIT ?
        """, (limit,)).fetchall()


def get_movies(filters, sort, page, page_size=20):
    offset = (page - 1) * page_size

    where = []
    params = []

    # --- uniquement des films ---
    where.append("m.titleType = 'movie'")

    # --- filtres ---
    if filters.get("year_min"):
        where.append("m.startYear >= ?")
        params.append(filters["year_min"])

    if filters.get("year_max"):
        where.append("m.startYear <= ?")
        params.append(filters["year_max"])

    if filters.get("min_rating"):
        where.append("r.averageRating >= ?")
        params.append(filters["min_rating"])

    if filters.get("genre"):
        where.append("g.genre = ?")
        params.append(filters["genre"])

    where_sql = "WHERE " + " AND ".join(where)

    # --- tri ---
    order_map = {
        "title": "m.primaryTitle",
        "year": "m.startYear",
        "rating": "r.averageRating",
    }

    order_by = order_map.get(sort.get("field", "title"))
    direction = "DESC" if sort.get("direction") == "desc" else "ASC"

    sql = f"""
        SELECT DISTINCT
            m.mid,
            m.primaryTitle,
            m.startYear,
            r.averageRating
        FROM movies m
        LEFT JOIN ratings r ON m.mid = r.mid
        LEFT JOIN genres g ON m.mid = g.mid
        {where_sql}
        ORDER BY
            CASE WHEN {order_by} IS NULL THEN 1 ELSE 0 END,
            CAST({order_by} AS REAL) {direction}
        LIMIT ? OFFSET ?
    """

    params.extend([page_size, offset])

    with get_connection() as conn:
        return conn.execute(sql, params).fetchall()


def count_movies_filtered(filters):
    where = []
    params = []
    # --- filtres ---
    where.append("m.titleType = 'movie'")


    if filters.get("year_min"):
        where.append("m.startYear >= ?")
        params.append(filters["year_min"])

    if filters.get("year_max"):
        where.append("m.startYear <= ?")
        params.append(filters["year_max"])

    if filters.get("min_rating"):
        where.append("r.averageRating >= ?")
        params.append(filters["min_rating"])

    if filters.get("genre"):
        where.append("g.genre = ?")
        params.append(filters["genre"])


    where_sql = "WHERE " + " AND ".join(where) if where else ""

    sql = f"""
        SELECT COUNT(DISTINCT m.mid)
        FROM movies m
        LEFT JOIN ratings r ON m.mid = r.mid
        LEFT JOIN genres g ON m.mid=g.mid
        {where_sql}
    """

    with get_connection() as conn:
        return conn.execute(sql, params).fetchone()[0]


def search_movies(query, limit=20):
    sql = """
        SELECT mid, primaryTitle, startYear
        FROM movies
        WHERE primaryTitle LIKE ?
        AND titleType = 'movie'
        ORDER BY startYear DESC
        LIMIT ?
    """
    return execute_query(sql, (f"%{query}%", limit))

def search_persons(query, limit=20):
    sql = """
        SELECT pid, name
        FROM persons
        WHERE name LIKE ?
        ORDER BY name
        LIMIT ?
    """
    return execute_query(sql, (f"%{query}%", limit))


# ---------- STATISTIQUES ----------

def movies_by_genre():
    with get_connection() as conn:
        return conn.execute("""
            SELECT g.genre, COUNT(*) as count
            FROM genres g
            JOIN movies m ON g.mid = m.mid
            WHERE m.titleType = 'movie'
            GROUP BY g.genre
            ORDER BY count DESC
        """).fetchall()


def movies_by_decade():
    with get_connection() as conn:
        return conn.execute("""
            SELECT (m.startYear / 10) * 10 AS decade,
                   COUNT(*) as count
            FROM movies m
            WHERE m.startYear IS NOT NULL
              AND m.titleType = 'movie'
            GROUP BY decade
            ORDER BY decade
        """).fetchall()


def ratings_distribution():
    with get_connection() as conn:
        return conn.execute("""
            SELECT ROUND(averageRating) AS rating,
                   COUNT(*) as count
            FROM ratings
            WHERE averageRating IS NOT NULL
            GROUP BY rating
            ORDER BY rating
        """).fetchall()


def top_actors(limit=10):
    with get_connection() as conn:
        return conn.execute("""
            SELECT p.name, COUNT(*) as count
            FROM principals pr
            JOIN persons p ON pr.pid = p.pid
            WHERE pr.category = 'actor'
            GROUP BY pr.pid
            ORDER BY count DESC
            LIMIT ?
        """, (limit,)).fetchall()

def get_all_genres():
    sql = """
        SELECT DISTINCT genre
        FROM genres
        ORDER BY genre
    """
    return execute_query(sql)
