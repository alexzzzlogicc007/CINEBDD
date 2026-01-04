from django.shortcuts import render
from math import ceil

from .services import sqlite_service
from .services import mongo_service


# =========================
# PAGE D'ACCUEIL
# =========================
def home(request):
    context = {
        "nb_movies": sqlite_service.count_movies(),
        "nb_actors": sqlite_service.count_actors(),
        "nb_directors": sqlite_service.count_directors(),
        "top_movies": sqlite_service.get_top_movies(),
        "random_movies": sqlite_service.get_random_movies(),
    }
    return render(request, "movies/home.html", context)


# =========================
# LISTE DES FILMS
# =========================
def movie_list(request):
    # page courante
    page = int(request.GET.get("page", 1))

    # filtres (nettoyés)
    filters = {
        "genre": request.GET.get("genre") or None,
        "year_min": request.GET.get("year_min") or None,
        "year_max": request.GET.get("year_max") or None,
        "min_rating": request.GET.get("min_rating") or None,
    }

    # tri
    sort = {
        "field": request.GET.get("sort", "title"),
        "direction": request.GET.get("dir", "asc"),
    }

    # données
    movies = sqlite_service.get_movies(filters, sort, page)
    total = sqlite_service.count_movies_filtered(filters)
    genres = sqlite_service.get_all_genres()

    # pagination
    total_pages = ceil(total / 20)
    window = 5
    start = max(page - window, 1)
    end = min(page + window, total_pages)

    context = {
        "movies": movies,
        "genres": genres,
        "page": page,
        "total_pages": total_pages,
        "page_range": range(start, end + 1),
        "filters": filters,
        "sort": sort,
    }

    return render(request, "movies/list.html", context)


# =========================
# DÉTAIL D'UN FILM (MongoDB)
# =========================
def movie_detail(request, movie_id):
    movie = mongo_service.get_movie_by_id(movie_id)

    if not movie:
        raise Http404("Film introuvable")

    # clé utile pour les templates
    movie["mid"] = movie["_id"]

    return render(request, "movies/detail.html", {
        "movie": movie
    })


# =========================
# RECHERCHE
# =========================
def search(request):
    query = request.GET.get("q", "").strip()

    movies = []
    persons = []

    if query:
        movies = sqlite_service.search_movies(query)
        persons = sqlite_service.search_persons(query)

    return render(request, "movies/search.html", {
        "query": query,
        "movies": movies,
        "persons": persons
    })


# =========================
# STATISTIQUES
# =========================
def stats(request):
    context = {
        "genres": sqlite_service.movies_by_genre(),
        "decades": sqlite_service.movies_by_decade(),
        "ratings": sqlite_service.ratings_distribution(),
        "actors": sqlite_service.top_actors(),
    }
    return render(request, "movies/stats.html", context)
