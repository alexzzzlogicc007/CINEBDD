from django.http import JsonResponse
from movies.services import mongo_service, sqlite_service


def stats_view(request):
    """
    Vue de test affichant des statistiques simples
    pour vérifier l'intégration SQLite + MongoDB.
    """

    data = {
        "sqlite": {
            "movies": sqlite_service.count_movies(),
            "persons": sqlite_service.count_persons(),
        },
        "mongodb": {
            "movies": mongo_service.count_movies(),
            "persons": mongo_service.count_persons(),
            "top_genres": mongo_service.top_genres(),
        }
    }

    return JsonResponse(data)
