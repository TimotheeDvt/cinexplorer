from django.shortcuts import render
from .services import sqlite_service, mongo_service

def test_stats_view(request):
    # Données provenant de SQLite
    sql_stats = sqlite_service.get_db_stats()

    # Données provenant de MongoDB
    top_movies = mongo_service.get_top_rated_movies(5)

    context = {
        'sql_stats': sql_stats,
        'top_movies': top_movies,
        'db_status': 'Connected to SQLite & MongoDB Replica Set'
    }
    return render(request, 'movies/test_stats.html', context)