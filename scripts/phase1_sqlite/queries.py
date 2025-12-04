import sqlite3

###############################################
# 1. Filmographie d’un acteur
###############################################
def query_actor_filmography(conn, actor_name: str) -> list:
    """
    Retourne tous les films dans lesquels un acteur donné a joué.
    
    Args:
        conn: Connexion SQLite
        actor_name: Nom de l’acteur (ex: "Tom Hanks")
    
    Returns:
        Liste de tuples (titre, année, catégorie, job)
    
    SQL utilisé:
    SELECT m.primaryTitle, m.startYear, p.category, p.job
    FROM principals p
    JOIN persons pe ON pe.pid = p.pid
    JOIN movies m ON m.mid = p.mid
    WHERE pe.name LIKE ?
    ORDER BY m.startYear DESC;
    """
    sql = """
    SELECT m.primaryTitle, m.startYear, p.category, p.job
    FROM principals p
    JOIN persons pe ON pe.pid = p.pid
    JOIN movies m ON m.mid = p.mid
    WHERE pe.name LIKE ?
    ORDER BY m.startYear DESC;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


###############################################
# 2. Top N films d’un genre sur une période
###############################################
def query_top_n_films(conn, genre: str, start: int, end: int, n: int) -> list:
    """
    Retourne les N meilleurs films d'un genre donné entre deux années.
    
    Args:
        conn: Connexion SQLite
        genre: Genre du film
        start: Année minimale
        end: Année maximale
        n: Nombre de films à retourner
    
    Returns:
        Liste de tuples (titre, année, note)
    """
    sql = """
    SELECT m.primaryTitle, m.startYear, r.averageRating
    FROM movies m
    JOIN genres g ON g.mid = m.mid
    JOIN ratings r ON r.mid = m.mid
    WHERE g.genre = ?
      AND m.startYear BETWEEN ? AND ?
    ORDER BY r.averageRating DESC
    LIMIT ?;
    """
    return conn.execute(sql, (genre, start, end, n)).fetchall()


###############################################
# 3. Acteurs ayant joué plusieurs rôles
###############################################
def query_multi_role_actors(conn) -> list:
    """
    Acteurs ayant joué plusieurs rôles dans un même film.
    (On considère category/job comme rôle)
    
    Returns:
        Liste (nom acteur, titre film, nb_roles)
    """
    sql = """
    SELECT pe.name, m.primaryTitle, COUNT(*) AS nb_roles
    FROM principals p
    JOIN persons pe ON pe.pid = p.pid
    JOIN movies m ON m.mid = p.mid
    GROUP BY p.mid, p.pid
    HAVING COUNT(*) > 1
    ORDER BY nb_roles DESC;
    """
    return conn.execute(sql).fetchall()


###############################################
# 4. Collaborations réalisateur / acteur
###############################################
def query_collaborations(conn, actor_name: str) -> list:
    """
    Liste des réalisateurs ayant travaillé avec un acteur donné,
    avec nombre de collaborations.
    
    Uses: sous-requête.
    
    Returns:
        (nom_réalisateur, nb_collaborations)
    """
    sql = """
    SELECT d.name, COUNT(*) AS collaborations
    FROM directors di
    JOIN persons d ON d.pid = di.pid
    WHERE di.mid IN (
        SELECT p.mid
        FROM principals p
        JOIN persons pe ON pe.pid = p.pid
        WHERE pe.name LIKE ?
    )
    GROUP BY d.pid
    ORDER BY collaborations DESC;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


###############################################
# 5. Genres populaires
###############################################
def query_popular_genres(conn) -> list:
    """
    Genres ayant une note moyenne > 7.0 et plus de 50 films.
    
    Returns:
        (genre, nb_films, note_moyenne)
    """
    sql = """
    SELECT g.genre,
           COUNT(*) AS nb_films,
           AVG(r.averageRating) AS note_moyenne
    FROM genres g
    JOIN ratings r ON r.mid = g.mid
    GROUP BY g.genre
    HAVING AVG(r.averageRating) > 7.0
       AND COUNT(*) > 50
    ORDER BY note_moyenne DESC;
    """
    return conn.execute(sql).fetchall()


###############################################
# 6. Évolution de carrière d’un acteur (par décennie)
###############################################
def query_career_evolution(conn, actor_name: str) -> list:
    """
    Nombre de films par décennie pour un acteur donné,
    avec moyenne des notes.
    
    Utilise un CTE (WITH).
    
    Returns:
        (décennie, nb_films, note_moyenne)
    """
    sql = """
    WITH actor_movies AS (
        SELECT m.startYear AS year, r.averageRating AS rating
        FROM principals p
        JOIN persons pe ON pe.pid = p.pid
        JOIN movies m ON m.mid = p.mid
        LEFT JOIN ratings r ON r.mid = m.mid
        WHERE pe.name LIKE ?
    )
    SELECT (year/10)*10 AS decade,
           COUNT(*) AS nb_films,
           AVG(rating) AS note_moyenne
    FROM actor_movies
    WHERE year IS NOT NULL
    GROUP BY decade
    ORDER BY decade;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


###############################################
# 7. Classement des 3 meilleurs films par genre
###############################################
def query_top3_by_genre(conn) -> list:
    """
    Pour chaque genre : top 3 des films classés par note moyenne.
    Utilise RANK().
    """
    sql = """
    SELECT genre, primaryTitle, averageRating, rank
    FROM (
        SELECT g.genre,
               m.primaryTitle,
               r.averageRating,
               RANK() OVER (
                   PARTITION BY g.genre
                   ORDER BY r.averageRating DESC
               ) AS rank
        FROM genres g
        JOIN movies m ON m.mid = g.mid
        JOIN ratings r ON r.mid = m.mid   -- ✔ CORRECTION ICI
    )
    WHERE rank <= 3
    ORDER BY genre, rank;
    """
    return conn.execute(sql).fetchall()


###############################################
# 8. Carrière propulsée
###############################################
def query_career_boost(conn) -> list:
    """
    Personnes ayant percé grâce à un film.
    Définition :
      - avant : films < 200k votes
      - après : films > 200k votes
    
    Returns:
        (nom, nb_films_avant, nb_films_apres)
    """
    sql = """
    SELECT pe.name,
           SUM(CASE WHEN r.numVotes < 200000 THEN 1 ELSE 0 END) AS avant,
           SUM(CASE WHEN r.numVotes >= 200000 THEN 1 ELSE 0 END) AS apres
    FROM principals p
    JOIN persons pe ON pe.pid = p.pid
    JOIN ratings r ON r.mid = p.mid
    GROUP BY pe.pid
    HAVING apres > 0
    ORDER BY apres DESC;
    """
    return conn.execute(sql).fetchall()


###############################################
# 9. Requête libre
###############################################
def query_custom(conn) -> list:
    """
    Requête libre :
    Films où un réalisateur et un scénariste ont aussi joué dedans.
    
    Returns:
        (titre, realisateur, scenariste)
    """
    sql = """
    SELECT DISTINCT m.primaryTitle, d.name, w.name
    FROM movies m
    JOIN directors di ON di.mid = m.mid
    JOIN persons d ON d.pid = di.pid
    JOIN writers wr ON wr.mid = m.mid
    JOIN persons w ON w.pid = wr.pid
    JOIN principals p ON p.mid = m.mid
    WHERE p.pid = di.pid
      OR p.pid = wr.pid;
    """
    return conn.execute(sql).fetchall()


