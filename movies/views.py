from django.shortcuts import render
from .services import sqlite_service, mongo_service

def home_view(request):
    # Statistiques (SQLite)
    stats = sqlite_service.get_db_stats()

    # Top 10 (MongoDB)
    top_movies = mongo_service.get_top_rated_movies(10)

    # Films aléatoires (SQLite)
    random_movies = sqlite_service.get_random_movies(6)

    context = {
        'stats': stats,
        'top_movies': top_movies,
        'random_movies': random_movies,
    }
    return render(request, 'movies/home.html', context)

def movie_detail_view(request, movie_id):
    movie = mongo_service.get_movie_detail(movie_id)

    print(movie)

    if not movie:
        return render(request, '404.html', {'message': "Film non trouvé"}, status=404)

    similar_movies = mongo_service.get_similar_movies(movie)

    return render(request, 'movies/detail.html', {
        'movie': movie,
        'similar_movies': similar_movies
    })

def movie_list_view(request):
    # Récupération et nettoyage des paramètres facultatifs
    def get_opt(param):
        val = request.GET.get(param)
        return val if val and val.strip() != '' else None

    page = int(request.GET.get('page', 1))
    genre = get_opt('genre')
    year_min = get_opt('year_min')
    year_max = get_opt('year_max')
    min_rating = get_opt('min_rating')
    sort_by = request.GET.get('sort_by', 'title')
    order = request.GET.get('order', 'ASC')
    view_type = request.GET.get('view', 'list')

    movies = sqlite_service.get_filtered_movies(
        page=page, genre=genre, year_min=year_min,
        year_max=year_max, min_rating=min_rating,
        sort_by=sort_by, order=order
    )

    genres = sqlite_service.get_available_genres()

    context = {
        'movies': movies,
        'genres': genres,
        'page': page,
        'next_page': page + 1,
        'prev_page': page - 1 if page > 1 else None,
        'view_type': view_type,
        'current_params': {k: v for k, v in request.GET.items() if v} # Garde uniquement les filtres actifs
    }
    return render(request, 'movies/list.html', context)

def search_view(request):
    query = request.GET.get('q', '').strip()
    results = sqlite_service.search_entities(query) if query else {'movies': [], 'persons': []}

    context = {
        'results': results,
        'query': query,
    }
    return render(request, 'movies/search.html', context)

from django.http import JsonResponse

def stats_api(request):
    # Récupération des données via les services SQLite et MongoDB
    sqlite_data = sqlite_service.get_stats_data()
    ratings_data = mongo_service.get_rating_distribution()

    return JsonResponse({
        'genres': sqlite_data['genres'],
        'decades': sqlite_data['decades'],
        'actors': sqlite_data['actors'],
        'ratings': ratings_data,
    })

def stats_view(request):
    """Affiche uniquement la structure de la page (très rapide)."""
    return render(request, 'movies/stats.html')