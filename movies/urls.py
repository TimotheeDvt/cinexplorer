from django.urls import path
from .views import test_stats_view

urlpatterns = [
    path('test-db/', test_stats_view, name='test_db_connection'),
]