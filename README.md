# CineExplorer

## Présentation

**CineExplorer** est une plateforme web de découverte de films basée sur une architecture
**multi-bases**, combinant **SQLite** (requêtes rapides, index, jointures) et **MongoDB**
(documents complets pour les détails).
Le projet couvre l’import IMDB (CSV), les requêtes, les benchmarks, la migration vers MongoDB,
la réplication (Replica Set) et une interface web développée avec **Django**.

Projet réalisé dans le cadre du module **4A-BDA – Bases de Données Avancées**
à **Polytech Marseille**.

---

## Structure du projet

```
cineexplorer/
├── config/                     # Configuration Django
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
├── data/
│   ├── csv/                    # Fichiers CSV IMDB
│   ├── imdb.db                 # Base SQLite
│   └── mongo/
│       ├── standalone/         # Données MongoDB (Phase 2)
│       ├── db-1/               # Replica Set - Nœud 1
│       ├── db-2/               # Replica Set - Nœud 2
│       └── db-3/               # Replica Set - Nœud 3
├── scripts/
│   ├── phase1_sqlite/
│   │   ├── create_schema.py    # Creation du schema SQLite
│   │   ├── import_data.py      # Import des CSV vers SQLite
│   │   ├── queries.py          # 9 requetes SQL
│   │   └── benchmark.py        # Tests de performance
│   ├── phase2_mongodb/
│   │   ├── migrate_flat.py     # Migration vers MongoDB (collections plates)
│   │   ├── queries_mongo.py    # 9 requetes MongoDB
│   │   ├── migrate_structured.py # Creation de movies_complete
│   │   └── compare_performance.py # Comparaison SQLite vs MongoDB
│   └── phase3_replica/
│       ├── init_replica.py     # Initialisation du Replica Set (Python)
│       ├── test_failover.py    # Tests de tolerance aux pannes
│       ├── import_to_replica.py # Import des donnees dans le Replica Set
│       └── setup_replica.bat   # Script de configuration (Windows)
│   ├── services/
│   │   ├── sqlite_service.py   # Service d'acces SQLite
│   │   └── mongo_service.py    # Service d'acces MongoDB
│   ├── templates/movies/
│   │   ├── base.html           # Template de base
│   │   ├── home.html           # Page d'accueil
│   │   ├── list.html           # Liste des films
│   │   ├── detail.html         # Detail d'un film
│   │   ├── search.html         # Recherche
│   │   └── stats.html          # Statistiques
│   ├── views.py
│   └── urls.py
├── static/                     # Fichiers statiques
├── manage.py
├── requirements.txt
└── reports/                    # Rapports PDF

```

---

## Prérequis

- Python 3.10+
- MongoDB Community Edition 8.0
- pip

---

## Installation

```bash
git clone <url_du_repo>
cd cineexplorer
pip install -r requirements.txt
```

Placer les fichiers CSV IMDB dans :
```
data/csv/
```

---

## Phase 1 – SQLite

```bash
cd scripts/phase1_sqlite
python create_schema.py
python import_data.py
python queries.py
python benchmark.py
```

---

## Phase 2 – MongoDB (Standalone)

```bash
mongod --dbpath ./data/mongo/standalone
cd scripts/phase2_mongodb
python migrate_flat.py
python migrate_structured.py
python compare_performance.py
```

---

## Phase 3 – MongoDB Replica Set

```bash
mongod --replSet rs0 --port 27017 --dbpath ./data/mongo/db-1 --bind_ip localhost
mongod --replSet rs0 --port 27018 --dbpath ./data/mongo/db-2 --bind_ip localhost
mongod --replSet rs0 --port 27019 --dbpath ./data/mongo/db-3 --bind_ip localhost
```

```bash
cd scripts/phase3_replica
python init_replica.py
python import_to_replica.py
python test_failover.py
```

---

## Phase 4 – Application Web Django

```bash
python manage.py runserver
```

Accessible sur : http://127.0.0.1:8000

---

## Pages disponibles

| URL | Description |
|---|---|
| `/` | Page d’accueil avec statistiques |
| `/movies/` | Liste des films avec filtres |
| `/movies/<id>/` | Détail complet d’un film |
| `/search/` | Recherche films et personnes |
| `/stats/` | Statistiques et graphiques |

---

## Fonctionnalités principales

- Pagination (20 films par page)
- Filtres (genre, période, note minimale)
- Recherche multi-critères
- Détails complets depuis MongoDB
- Statistiques interactives (Chart.js)

---

## Stratégie multi-bases

| Fonctionnalité | Base utilisée | Justification |
|---|---|---|
| Listes et filtres | SQLite | Jointures et index efficaces |
| Recherche | SQLite | LIKE rapide |
| Statistiques | SQLite | Agrégations performantes |
| Détail d’un film | MongoDB | Document complet |
| Films similaires | MongoDB | Flexibilité NoSQL |

---

## Architecture du Replica Set
                    ┌─────────────────┐
                    │   Application   │
                    │     Django      │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
       ┌──────────┐   ┌──────────┐   ┌──────────┐
       │ Primary  │   │Secondary │   │Secondary │
       │  :27017  │   │  :27018  │   │  :27019  │
       │  db-1    │   │  db-2    │   │  db-3    │
       └──────────┘   └──────────┘   └──────────┘
              │              ▲              ▲
              └──────────────┴──────────────┘
                     Replication

## Technologies
Backend : Django 4.x, Python 3.10+
Bases de donnees : SQLite, MongoDB 8.0
Frontend : Bootstrap 5, Chart.js
Driver MongoDB : PyMongo
## Auteur

**ABDALLAH Alex**  
