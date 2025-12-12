import sqlite3
import time
from typing import Tuple, Any, Dict, Callable

# Configuration
DB_PATH = "data/imdb.db"

def time_query(conn: sqlite3.Connection, func: Callable, *args, **kwargs) -> Tuple[Any, float]:
    """
    Exécute une fonction de requête, mesure son temps, et retourne le résultat et le temps d'exécution.
    """
    start_time = time.time()
    results = func(conn, *args, **kwargs)
    end_time = time.time()
    elapsed_time = end_time - start_time

    return results, elapsed_time


# --------------------------------------------------------------------------
# Fonctions de Requête SQL
# --------------------------------------------------------------------------

def query_actor_filmography(conn: sqlite3.Connection, actor_name: str) -> list:
    """Récupère la filmographie complète d'un acteur, triée par année décroissante."""
    sql = """
        SELECT m.primaryTitle, m.startYear, c.name
        FROM Movies m
        JOIN Characters c ON m.mid = c.mid
        JOIN Persons pe ON c.pid = pe.pid
        WHERE pe.primaryName LIKE ?  -- Utilisation de LIKE pour une recherche flexible
        ORDER BY m.startYear DESC
    """
    search_param = f'%{actor_name}%'
    cursor = conn.cursor()
    cursor.execute(sql, (search_param,))
    return cursor.fetchall()

def query_top_n_films(conn: sqlite3.Connection, genre: str, startYear: int, endYear: int, n: int) -> list:
    """Trouve les N films les mieux notés pour un genre donné et une période spécifique."""
    sql = """
        SELECT m.primaryTitle, m.startYear, g.genre, r.averageRating
        FROM Movies m
        JOIN Genres g ON m.mid = g.mid
        JOIN Ratings r ON m.mid = r.mid
        WHERE g.genre LIKE ?
          AND m.startYear BETWEEN ? AND ?
        ORDER BY r.averageRating DESC
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (f'%{genre}%', startYear, endYear, n))
    return cursor.fetchall()

def query_actor_multi_roles(conn: sqlite3.Connection, n: int) -> list:
    """Trouve les N acteurs qui jouent plusieurs rôles différents dans le même film."""
    sql = """
        SELECT pe.primaryName, m.primaryTitle, COUNT(c.pid) as role_count
        FROM Persons pe
        JOIN Characters c ON pe.pid = c.pid
        JOIN Movies m ON c.mid = m.mid
        GROUP BY pe.pid, m.mid
        HAVING role_count > 1
        ORDER BY role_count DESC, pe.primaryName
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (n,))
    return cursor.fetchall()

def query_collaborations(conn: sqlite3.Connection, actor: str) -> list:
    """
    Liste les réalisateurs ayant collaboré avec un acteur spécifique,
    avec le nombre de films réalisés ensemble. Utilise une sous-requête pour trouver les films de l'acteur.
    """
    sql = """
        SELECT p_dir.primaryName AS Realisateur, COUNT(d.mid) AS Nombre_de_Films
        FROM Directors d
        JOIN Persons p_dir ON d.pid = p_dir.pid
        WHERE d.mid IN (
            -- Sous-requête : trouve tous les MID des films où l'acteur a un rôle
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

def query_genre_popularity(conn: sqlite3.Connection, n: int) -> list:
    """Trouve les N genres les plus populaires : note moyenne > 7.0 et plus de 50 films."""
    sql = """
        SELECT g.genre, AVG(r.averageRating) AS avg_rating, COUNT(m.mid) AS film_count
        FROM Genres g
        JOIN Ratings r ON g.mid = r.mid
        JOIN Movies m ON g.mid = m.mid
        GROUP BY g.genre
        HAVING avg_rating > 7.0 AND film_count > 50
        ORDER BY avg_rating DESC
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (n,))
    return cursor.fetchall()

def query_evolution_career(conn: sqlite3.Connection, actor_name: str) -> list:
    """
    Analyse l'évolution de la carrière d'un acteur : nombre de films et note moyenne par décennie.
    Utilise une Common Table Expression (CTE - WITH).
    """
    sql = """
        WITH ActorMovies AS (
            SELECT m.startYear, r.averageRating
            FROM Persons pe
            JOIN Characters c ON pe.pid = c.pid
            JOIN Movies m ON c.mid = m.mid
            JOIN Ratings r ON m.mid = r.mid
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

def query_rank_by_genre(conn: sqlite3.Connection, genre: str) -> list:
    """Classe tous les films d'un genre donné en fonction de leur note moyenne, en utilisant la fonction de fenêtre RANK()."""
    sql = """
        SELECT primaryTitle, startYear, averageRating,
            RANK() OVER (PARTITION BY g.genre ORDER BY r.averageRating DESC) AS genre_rank
        FROM Movies m
        JOIN Genres g ON m.mid = g.mid
        JOIN Ratings r ON m.mid = r.mid
        WHERE g.genre LIKE ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (f'%{genre}%',))
    return cursor.fetchall()

def query_carreer_booster(conn: sqlite3.Connection, n: int) -> list:
    """
    Identifie les N personnes dont la carrière a été le plus 'boostée'
    (ratio films populaires / films peu connus).
    Définit un film populaire comme ayant plus de 200 000 votes.
    """
    sql = """
        WITH PersonMovieStats AS (
            SELECT pe.pid, pe.primaryName,
                SUM(CASE WHEN r.numVotes < 200000 THEN 1 ELSE 0 END) AS before_count, -- Films avant le "break"
                SUM(CASE WHEN r.numVotes >= 200000 THEN 1 ELSE 0 END) AS after_count   -- Films après le "break"
            FROM Persons pe
            JOIN Principals p ON pe.pid = p.pid
            JOIN Movies m ON p.mid = m.mid
            JOIN Ratings r ON m.mid = r.mid
            GROUP BY pe.pid
        )
        SELECT primaryName, before_count, after_count
        FROM PersonMovieStats
        WHERE before_count > 0 AND after_count > 0
        ORDER BY (CAST(after_count AS FLOAT) / before_count) DESC -- Tri par ratio pour trouver le plus grand "saut" de carrière
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (n,))
    return cursor.fetchall()

def query_free_form(conn: sqlite3.Connection) -> list:
    """Exemple de requête libre : Les 10 films les mieux notés (note > 8.0) avec les acteurs principaux."""
    sql = """
        SELECT pe.primaryName AS actor_name, m.primaryTitle AS movie_title, r.averageRating
        FROM Persons pe
        JOIN Principals p ON pe.pid = p.pid
        JOIN Movies m ON p.mid = m.mid
        JOIN Ratings r ON m.mid = r.mid
        WHERE r.averageRating > 8.0
        ORDER BY r.averageRating DESC
        LIMIT 10
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


# --------------------------------------------------------------------------
# Fonction Principale de Benchmark
# --------------------------------------------------------------------------

def main():
    conn = None

    # Résultats de benchmark 'avant' (à remplacer par les vrais résultats après la première exécution sans index)
    # Ceci simule une mesure de référence avant l'optimisation par index.
    benchmark_results: Dict[str, Dict[str, float]] = {
        'Filmography': {'before': 18.003103733062744},
        'Top_N_Films': {'before': 0.34579944610595703},
        'Multi_Roles': {'before': 20.788132905960083},
        'Collaborations': {'before': 33.144147634506226},
        'Genre_Pop': {'before': 1.5411641597747803},
        'Career_Evol': {'before': 15.812682151794434},
        'Rank_by_Genre': {'before': 0.9258627891540527},
        'Career_Booster': {'before': 42.09673237800598},
        'Free_Form': {'before': 7.226547002792358}
    }

    try:
        conn = sqlite3.connect(DB_PATH)

        print("🚀 Démarrage du benchmark des requêtes (après optimisation par index)...")
        print("-" * 50)

        # Exécuter et chronométrer chaque requête (Temps 2)
        print("Exécution des requêtes pour l'obtention du Temps 2...")

        _, t1 = time_query(conn, query_actor_filmography, actor_name="Tom Hanks")
        benchmark_results['Filmography']["after"] = t1

        _, t2 = time_query(conn, query_top_n_films, genre="Drama", startYear=1990, endYear=2000, n=5)
        benchmark_results['Top_N_Films']["after"] = t2

        _, t3 = time_query(conn, query_actor_multi_roles, n=10)
        benchmark_results['Multi_Roles']["after"] = t3

        # Le résultat de la collaboration est affiché pour l'exemple
        collaborations_result, t4 = time_query(conn, query_collaborations, actor="Tom Hanks")
        benchmark_results['Collaborations']["after"] = t4
        print(f"Exemple 'Collaborations' (Tom Hanks): {collaborations_result[:3]}...")

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

        # Affichage des résultats bruts
        # print("\nDonnées de benchmark après exécution:")
        # print(benchmark_results)

        print("-" * 50)
        print("✅ Benchmark terminé.")
        print("\n--- Synthèse des Temps d'Exécution (Avant/Après Optimisation) ---")

        # Affichage formaté des résultats
        print("{:<20} {:<10} {:<10} {:<10}".format('Requête', 'Temps 1 (s)', 'Temps 2 (s)', 'Gain (%)'))
        print("{:-<20} {:-<10} {:-<10} {:-<10}".format('', '', '', ''))
        for k, v in benchmark_results.items():
            gain_percent = (v["before"] - v["after"]) / v["before"] * 100
            print(f"{k:<20} {v["before"]:<10.4f} {v["after"]:<10.4f} {gain_percent:<10.2f}")


    except sqlite3.Error as e:
        print(f"Erreur SQLite : {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()