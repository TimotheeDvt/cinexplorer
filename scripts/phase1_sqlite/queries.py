import sqlite3
import pandas as pd # Inutile pour cet exemple, mais conservé.

DB_PATH = "data/imdb.db"

def main():
    conn = sqlite3.connect(DB_PATH)

    result = query(conn)
    if not result:
        print("Aucun résultat trouvé.")
        return

    print(result)

    if conn:
        conn.close()

def query_actor_filmography(conn, actor_name: str) -> list:
    sql = """
        SELECT m.primaryTitle, m.startYear, c.name
        FROM movies m
        JOIN persons pe ON c.pid = pe.pid -- Jointure avec la personne (nom de l'acteur)
        JOIN characters c ON m.mid = c.mid AND c.pid = pe.pid
        WHERE pe.primaryName LIKE ?
        ORDER BY m.startYear DESC
    """
    search_param = f'%{actor_name}%'

    cursor = conn.cursor()
    cursor.execute(sql, (search_param,))
    return cursor.fetchall()

def query_top_n_films(conn, genre, startYear, endYear, n) -> list:
    sql = """
        SELECT m.primaryTitle, m.startYear, g.genre, r.averageRating
        FROM movies m
        JOIN genres g ON m.mid = g.mid
        JOIN ratings r ON m.mid = r.mid
        WHERE g.genre LIKE ?
          AND m.startYear BETWEEN ? AND ?
        ORDER BY r.averageRating DESC
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (f'%{genre}%', startYear, endYear, n))
    return cursor.fetchall()

def query_actor_multi_roles(conn, n) -> list:
    # Cette requête trouve les acteurs qui plusieurs roles dans un meme film triés par nombre de roles
    sql = """
        SELECT pe.primaryName, m.primaryTitle, COUNT(c.pid) as role_count
        FROM persons pe
        JOIN characters c ON pe.pid = c.pid
        JOIN movies m ON c.mid = m.mid
        GROUP BY pe.pid, m.mid
        HAVING role_count > 1
        ORDER BY role_count DESC, pe.primaryName
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (n,))
    return cursor.fetchall()

def query_collaborations(conn, real: str, actor: str) -> list:
    # Réalisateurs ayant travaillé avec un acteur spécifique, avec le nombre de films ensemble (utiliser une sous-requête
    sql = """
        SELECT pe.primaryName AS director_name, COUNT(m.mid) AS collaboration_count
        FROM persons pe
        JOIN principals p ON pe.pid = p.pid
        JOIN movies m ON p.mid = m.mid
        WHERE pe.primaryName LIKE ?
          AND m.mid IN (
              SELECT m2.mid
              FROM persons pa
              JOIN principals p2 ON pa.pid = p2.pid
              JOIN movies m2 ON p2.mid = m2.mid
              WHERE pa.primaryName LIKE ?
            )
        GROUP BY pe.pid
        ORDER BY collaboration_count DESC
    """
    cursor = conn.cursor()
    cursor.execute(sql, (f'%{real}%', f'%{actor}%'))
    return cursor.fetchall()

def query_genre_popularity(conn, n) -> list:
    # Genres ayant une note moyenne >7.0 et plus de 50 films, triés par note (utiliser GROUP BY + HAVING)
    sql = """
        SELECT g.genre, AVG(r.averageRating) AS avg_rating, COUNT(m.mid) AS film_count
        FROM genres g
        JOIN ratings r ON g.mid = r.mid
        JOIN movies m ON g.mid = m.mid
        GROUP BY g.genre
        HAVING avg_rating > 7.0 AND film_count > 50
        ORDER BY avg_rating DESC
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (n,))
    return cursor.fetchall()

def query_evolution_career(conn, actor_name: str) -> list:
    # Pour un acteur donné, nombre de films par décennie avec note moyenne (utiliser CTE - WITH
    sql = """
        WITH ActorMovies AS (
            SELECT m.startYear, r.averageRating
            FROM persons pe
            JOIN characters c ON pe.pid = c.pid
            JOIN movies m ON c.mid = m.mid
            JOIN ratings r ON m.mid = r.mid
            WHERE pe.primaryName LIKE ?
        )
        SELECT (startYear / 10) * 10 AS decade, COUNT(*) AS film_count, AVG(averageRating) AS avg_rating
        FROM ActorMovies
        GROUP BY decade
        ORDER BY decade
    """
    cursor = conn.cursor()
    cursor.execute(sql, (f'%{actor_name}%',))
    return cursor.fetchall()

def query_rank_by_genre(conn, genre: str) -> list:
    # Pour chaque genre, les 3 meilleurs films avec leur rang (utiliser RANK() ou ROW_NUMBER()
    sql = """
        SELECT primaryTitle, startYear, averageRating,
            RANK() OVER (PARTITION BY g.genre ORDER BY r.averageRating DESC) AS genre_rank
        FROM movies m
        JOIN genres g ON m.mid = g.mid
        JOIN ratings r ON m.mid = r.mid
        WHERE g.genre LIKE ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (f'%{genre}%',))
    return cursor.fetchall()

def query_carreer_booster(conn, n: int) -> list:
    # Personnes ayant percé grâce à un film (avant : films <200k votes, après : films >200k votes
    sql = """
        WITH PersonMovieStats AS (
            SELECT pe.pid, pe.primaryName,
                SUM(CASE WHEN r.numVotes < 200000 THEN 1 ELSE 0 END) AS before_count,
                SUM(CASE WHEN r.numVotes >= 200000 THEN 1 ELSE 0 END) AS after_count
            FROM persons pe
            JOIN principals p ON pe.pid = p.pid
            JOIN movies m ON p.mid = m.mid
            JOIN ratings r ON m.mid = r.mid
            GROUP BY pe.pid
        )
        SELECT primaryName, before_count, after_count
        FROM PersonMovieStats
        WHERE before_count > 0 AND after_count > 0
        ORDER BY (CAST(after_count AS FLOAT) / before_count) DESC
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (n,))
    return cursor.fetchall()

def query_free_form(conn) -> list:
    # Exemples : Quels sont les 10 films les mieux notés (note > 8.0) avec le nom des acteurs principaux ?
    sql = """
        SELECT pe.primaryName AS actor_name, m.primaryTitle AS movie_title, r.averageRating
        FROM persons pe
        JOIN principals p ON pe.pid = p.pid
        JOIN movies m ON p.mid = m.mid
        JOIN ratings r ON m.mid = r.mid
        WHERE r.averageRating > 8.0
        ORDER BY r.averageRating DESC
        LIMIT 10
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def query(conn):
    # return query_actor_filmography(conn, actor_name="Tom Hanks")
    # return query_top_n_films(conn, genre="Drama", startYear=1990, endYear=2000, n=5)
    # return query_actor_multi_roles(conn, n=10)
    # return query_collaborations(conn, real="Steven Spielberg", actor="Tom Hanks")
    # return query_genre_popularity(conn, n=10)
    # return query_evolution_career(conn, actor_name="Leonardo DiCaprio")
    # return query_rank_by_genre(conn, genre="Comedy")
    # return query_carreer_booster(conn, n=10)
    return query_free_form(conn)

if __name__ == "__main__":
    main()