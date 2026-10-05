from django.urls import path

from fraud.views import dashboard, stats

urlpatterns = [
    path("", dashboard, name="dashboard"),
    path("api/stats/", stats, name="stats"),
]