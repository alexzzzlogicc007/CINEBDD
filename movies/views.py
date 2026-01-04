from django.shortcuts import render
from django.http import Http404
from math import ceil

from .services import sqlite_service
from .services import mongo_service

def home(request):
    context = {
        "nb_movies": sqlite_service.count_movies(),
        "nb_actors": sqlite_service.count_actors(),
        "nb_directors": sqlite_service.count_directors(),
        "top_movies": sqlite_service.get_top_movies(),
        "random_movies": sqlite_service.get_random_movies(),
    }
    return render(request, "movies/home.html", context)


def movie_list(request):
    page = int(request.GET.get("page", 1))

    filters = {
        "year_min": request.GET.get("year_min"),
        "year_max": request.GET.get("year_max"),
        "min_rating": request.GET.get("min_rating"),
    }

    sort = {
        "field": request.GET.get("sort", "title"),
        "direction": request.GET.get("dir", "asc"),
    }

    movies = sqlite_service.get_movies(filters, sort, page)
    total = sqlite_service.count_movies_filtered(filters)

    total_pages = ceil(total / 20)

    # --- pagination intelligente ---
    window = 5
    start = max(page - window, 1)
    end = min(page + window, total_pages)

    context = {
        "movies": movies,
        "page": page,
        "total_pages": total_pages,
        "page_range": range(start, end + 1),
        "filters": filters,
        "sort": sort,
    }

    return render(request, "movies/list.html", context)

def movie_detail(request, movie_id):
    movie = mongo_service.get_movie_by_id(movie_id)

    if movie:
        movie["mid"] = movie["_id"]   # 👈 clé magique

    return render(request, "movies/detail.html", {
        "movie": movie
    })


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


def stats(request):
    context = {
        "genres": sqlite_service.movies_by_genre(),
        "decades": sqlite_service.movies_by_decade(),
        "ratings": sqlite_service.ratings_distribution(),
        "actors": sqlite_service.top_actors(),
    }
    return render(request, "movies/stats.html", context)
