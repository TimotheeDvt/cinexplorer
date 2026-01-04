from django.urls import path
from .views import home_view, movie_detail_view, movie_list_view, search_view, stats_view, stats_api

urlpatterns = [
    path('', home_view, name='home'),
    path('movies/', movie_list_view, name='movie_list'),
    path('movies/<str:movie_id>/', movie_detail_view, name='movie_detail'),
    path('search/', search_view, name='search'),
    path('stats/', stats_view, name='stats'),
    path('api/stats/', stats_api, name='stats_api')
]