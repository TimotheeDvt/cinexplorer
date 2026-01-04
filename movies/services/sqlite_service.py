from django.db import connection
print(f"DEBUG: SQLite utilise -> {connection.settings_dict['NAME']}")
def get_db_stats():
    """Récupère des statistiques globales depuis SQLite."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM Movies")
        movie_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM Persons")
        person_count = cursor.fetchone()[0]

    return {
        'movie_count': movie_count,
        'person_count': person_count,
    }