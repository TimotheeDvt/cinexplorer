from django.db import connection
print(f"DEBUG: SQLite utilise -> {connection.settings_dict['NAME']}")
def get_db_stats():
    """Récupère des statistiques globales depuis SQLite."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM Movies")
        movie_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM Persons")
        person_count = cursor.fetchone()[0]

        # Compte les réalisateurs uniques dans la table Directors
        cursor.execute("SELECT COUNT(DISTINCT PID) FROM Directors")
        director_count = cursor.fetchone()[0]

    return {
        'movie_count': movie_count,
        'person_count': person_count,
        'director_count': director_count,
    }

def get_random_movies(limit=6):
    """Récupère des films aléatoires pour la page d'accueil."""
    with connection.cursor() as cursor:
        query = """
            SELECT m.MID AS movie_id, m.PRIMARYTITLE AS title, m.STARTYEAR AS year
            FROM Movies m
            ORDER BY RANDOM()
            LIMIT %s
        """
        cursor.execute(query, [limit])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

def get_filtered_movies(page=1, limit=20, genre=None, year_min=None, year_max=None, min_rating=None, sort_by='title', order='ASC'):
    offset = (page - 1) * limit

    # Mapping sécurisé pour le tri [cite: 162]
    sort_map = {
        'title': 'm.PRIMARYTITLE',
        'year': 'm.STARTYEAR',
        'rating': 'r.AVERAGERATING'
    }
    db_sort = sort_map.get(sort_by, 'm.PRIMARYTITLE')
    db_order = 'DESC' if order.upper() == 'DESC' else 'ASC'

    # Construction dynamique de la clause WHERE
    query_base = """
        SELECT DISTINCT m.MID AS movie_id, m.PRIMARYTITLE AS title, m.STARTYEAR AS year, r.AVERAGERATING AS average_rating
        FROM Movies m
        LEFT JOIN Ratings r ON m.MID = r.MID
        LEFT JOIN Genres g ON m.MID = g.MID
    """

    criteria = []
    params = []

    if genre:
        criteria.append("g.GENRE = %s")
        params.append(genre)
    if year_min:
        criteria.append("m.STARTYEAR >= %s")
        params.append(year_min)
    if year_max:
        criteria.append("m.STARTYEAR <= %s")
        params.append(year_max)
    if min_rating:
        criteria.append("r.AVERAGERATING >= %s")
        params.append(min_rating)

    # Assemblage de la requête
    if criteria:
        query_base += " WHERE " + " AND ".join(criteria)

    query_base += f" ORDER BY {db_sort} {db_order} LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with connection.cursor() as cursor:
        cursor.execute(query_base, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

def get_available_genres():
    """Récupère la liste des genres pour le filtre."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT GENRE FROM Genres ORDER BY GENRE")
        return [row[0] for row in cursor.fetchall()]

def search_all(query):
    """Recherche des films par titre et des personnes par nom dans SQLite."""
    with connection.cursor() as cursor:
        # Recherche de films
        cursor.execute("""
            SELECT MID AS movie_id, PRIMARYTITLE AS title, STARTYEAR AS year
            FROM Movies WHERE PRIMARYTITLE LIKE %s LIMIT 10
        """, [f'%{query}%'])
        movie_cols = [col[0] for col in cursor.description]
        movies = [dict(zip(movie_cols, row)) for row in cursor.fetchall()]

        # Recherche de personnes
        cursor.execute("""
            SELECT PID AS person_id, PRIMARYNAME AS name
            FROM Persons WHERE PRIMARYNAME LIKE %s LIMIT 10
        """, [f'%{query}%'])
        person_cols = [col[0] for col in cursor.description]
        persons = [dict(zip(person_cols, row)) for row in cursor.fetchall()]

    return {'movies': movies, 'persons': persons}

def search_entities(query):
    """
    Recherche par titre de film et par nom de personne dans SQLite.
    Les résultats sont limités pour la performance.
    """
    results = {'movies': [], 'persons': []}
    if not query:
        return results

    with connection.cursor() as cursor:
        # 1. Recherche par titre de film
        cursor.execute("""
            SELECT MID AS movie_id, PRIMARYTITLE AS title, STARTYEAR AS year
            FROM Movies
            WHERE PRIMARYTITLE LIKE %s
            LIMIT 20
        """, [f'%{query}%'])
        movie_cols = [col[0] for col in cursor.description]
        results['movies'] = [dict(zip(movie_cols, row)) for row in cursor.fetchall()]

        # 2. Recherche par nom de personne
        cursor.execute("""
            SELECT PID AS person_id, PRIMARYNAME AS name
            FROM Persons
            WHERE PRIMARYNAME LIKE %s
            LIMIT 20
        """, [f'%{query}%'])
        person_cols = [col[0] for col in cursor.description]
        results['persons'] = [dict(zip(person_cols, row)) for row in cursor.fetchall()]

    return results

def get_stats_data():
    """Récupère les données brutes pour les graphiques via SQLite."""
    with connection.cursor() as cursor:
        # 1. Films par genre (Bar Chart)
        cursor.execute("SELECT GENRE, COUNT(*) as count FROM Genres GROUP BY GENRE ORDER BY count DESC")
        genres = [dict(zip(['label', 'value'], row)) for row in cursor.fetchall()]

        # 2. Films par décennie (Line Chart)
        cursor.execute("""
            SELECT (STARTYEAR / 10) * 10 as decade, COUNT(*) 
            FROM Movies 
            WHERE STARTYEAR IS NOT NULL 
            GROUP BY decade ORDER BY decade
        """)
        decades = [dict(zip(['label', 'value'], row)) for row in cursor.fetchall()]

        # 3. Top 10 acteurs prolifiques (Bar Chart)
        cursor.execute("""
            SELECT p.PRIMARYNAME, COUNT(pr.MID) as count
            FROM Persons p
            JOIN Principals pr ON p.PID = pr.PID
            WHERE pr.CATEGORY IN ('actor', 'actress')
            GROUP BY p.PID ORDER BY count DESC LIMIT 10
        """)
        actors = [dict(zip(['label', 'value'], row)) for row in cursor.fetchall()]

    return {'genres': genres, 'decades': decades, 'actors': actors}