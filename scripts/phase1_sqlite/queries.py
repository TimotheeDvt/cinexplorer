import sqlite3
import time
from typing import Tuple, Any, Dict

DB_PATH = "data/imdb.db"

def time_query(conn, func, *args, **kwargs) -> Tuple[Any, float]:
    """
    Exécute une fonction de requête, mesure son temps, et retourne le résultat et le temps.
    """
    start_time = time.time()
    results = func(conn, *args, **kwargs)
    end_time = time.time()
    elapsed_time = end_time - start_time

    return results, elapsed_time


def main():
    conn = None

    benchmark_results: Dict[str, float] = {'Filmography': {'before': 18.003103733062744}, 'Top_N_Films': {'before': 0.34579944610595703}, 'Multi_Roles': {'before': 20.788132905960083}, 'Collaborations': {'before': 33.144147634506226}, 'Genre_Pop': {'before': 1.5411641597747803}, 'Career_Evol': {'before': 15.812682151794434}, 'Rank_by_Genre': {'before': 0.9258627891540527}, 'Career_Booster': {'before': 42.09673237800598}, 'Free_Form': {'before': 7.226547002792358}}

    try:
        conn = sqlite3.connect(DB_PATH)

        print("🚀 Démarrage du benchmark des requêtes...")
        print("-" * 50)

        # Exécuter et chronométrer chaque requête

        _, t1 = time_query(conn, query_actor_filmography, actor_name="Tom Hanks")
        benchmark_results['Filmography']["after"] = t1

        _, t2 = time_query(conn, query_top_n_films, genre="Drama", startYear=1990, endYear=2000, n=5)
        benchmark_results['Top_N_Films']["after"] = t2

        _, t3 = time_query(conn, query_actor_multi_roles, n=10)
        benchmark_results['Multi_Roles']["after"] = t3

        _, t4 = time_query(conn, query_collaborations, actor="Tom Hanks")
        benchmark_results['Collaborations']["after"] =t4

        print(_)

        _, t5 = time_query(conn, query_genre_popularity, n=10)
        benchmark_results['Genre_Pop']["after"] = t5

        _, t6 = time_query(conn, query_evolution_career, actor_name="Leonardo DiCaprio")
        benchmark_results['Career_Evol']["after"] = t6

        _, t7 = time_query(conn, query_rank_by_genre, genre="Comedy")
        benchmark_results['Rank_by_Genre']["after"] = t7

        _, t8 = time_query(conn, query_carreer_booster, n=10)
        benchmark_results['Career_Booster']["after"] = t8

        _, t9 = time_query(conn, query_free_form)
        benchmark_results['Free_Form']["after"] = t9

        print(benchmark_results)

        print("-" * 50)
        print("✅ Benchmark terminé.")
        print("\n--- Résultats des Temps d'Exécution ---")

        print("{:<20} {:<10} {:<10} {:<10}".format('Requête', 'Temps 1', 'Temps 2', 'Gain (%)'))
        print("{:-<20} {:-<10} {:-<10} {:-<10}".format('', '', '', ''))
        for k, v in benchmark_results.items():
            print(f"{k:<20} {v["before"]:<10.4f} {v["after"]:<10.4f} {(abs(v["before"] - v["after"]) / v["before"] * 100):<10.2f}")


    except sqlite3.Error as e:
        print(f"Erreur SQLite : {e}")
    finally:
        if conn:
            conn.close()

# --------------------------------------------------------------------------
# Fonctions de Requête
# --------------------------------------------------------------------------

def query_actor_filmography(conn, actor_name: str) -> list:
    sql = """
        SELECT m.primaryTitle, m.startYear, c.name
        FROM movies m
        JOIN characters c ON m.mid = c.mid
        JOIN persons pe ON c.pid = pe.pid
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

def query_collaborations(conn, actor: str) -> list:
    # Réalisateurs ayant travaillé avec un acteur spécifique, avec le nombre de films ensemble (utiliser une sous-requête)
    sql = """
        SELECT p_dir.primaryName AS Realisateur, COUNT(d.mid) AS Nombre_de_Films
        FROM Directors d
        JOIN Persons p_dir ON d.pid = p_dir.pid
        JOIN Movies m ON d.mid = m.mid
        WHERE d.mid IN (
            SELECT c.mid
            FROM Characters c
            JOIN Persons p_act ON c.pid = p_act.pid
            WHERE p_act.primaryName = ?
        )
        GROUP BY p_dir.pid
        ORDER BY Nombre_de_Films DESC, Realisateur ASC;
    """
    cursor = conn.cursor()
    cursor.execute(sql, (actor,))
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

if __name__ == "__main__":
    main()