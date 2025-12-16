import pymongo
from typing import List, Dict, Optional, Any
import time
import json

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "IMDB_DB"

TARGET_COLLECTION = "movies_complete"

def get_db_connection(uri: str, db_name: str) -> Optional[pymongo.database.Database]:
    """Établit la connexion à MongoDB et retourne l'objet Database."""
    client = None
    try:
        client = pymongo.MongoClient(uri)
        client.admin.command('ping')
        return client[db_name]
    except pymongo.errors.ConnectionFailure:
        print(f"❌ Erreur de connexion à MongoDB. Assurez-vous que le serveur est démarré à {uri}.")
        if client:
            client.close()
        return None

# --- Décorateur pour chronométrer ---
def time_query(func):
    """Décorateur pour chronométrer l'exécution d'une fonction de requête."""
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        query_name = func.__name__

        time_results = kwargs.get('time_results')
        if time_results is not None and isinstance(time_results, dict):
            time_results[query_name]["mongoNew"] = elapsed_time

        return result
    return wrapper


@time_query
def query_1_filmography_by_actor(db: pymongo.database.Database, actor_name: str, time_results: Dict) -> List[Dict]:
    """
    1. Filmographie d'un acteur : Dans quels films a joué un acteur donné ?
    (Recherche simple sur le tableau 'cast')
    """
    print(f"\n--- Exécution de la Requête 1: Filmographie de '{actor_name}' ---")

    pipeline = [
        # 1. Filtrer les documents où l'acteur est présent dans le tableau 'cast'
        {
            "$match": {
                "cast.name": actor_name
            }
        },
        # 2. Projection pour n'afficher que le titre et l'année
        {
            "$project": {
                "_id": 0,
                "title": 1,
                "year": 1
            }
        },
        # 3. Tri par année
        {
            "$sort": { "year": -1 }
        }
    ]

    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

@time_query
def query_2_top_n_movies_by_genre(db: pymongo.database.Database, genre: str, start_year: int, end_year: int, N: int, time_results: Dict) -> List[Dict]:
    """
    2. Top N films : Les N meilleurs films d'un genre sur une période selon la note moyenne.
    """
    print(f"\n--- Exécution de la Requête 2: Top {N} films {genre} ({start_year}-{end_year}) ---")

    pipeline = [
        # 1. Filtrer par genre et période
        {
            "$match": {
                "genre": genre,
                "year": { "$gte": start_year, "$lte": end_year },
                "rating.average": { "$exists": True, "$ne": None }
            }
        },
        # 2. Tri par note moyenne décroissante
        {
            "$sort": { "rating.average": -1 }
        },
        # 3. Limiter à N résultats
        {
            "$limit": N
        },
        # 4. Projection
        {
            "$project": {
                "_id": 0,
                "title": 1,
                "year": 1,
                "rating.average": 1
            }
        }
    ]
    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

@time_query
def query_3_multi_role_actors(db: pymongo.database.Database, time_results: Dict) -> List[Dict]:
    """
    3. Acteurs multi-rôles : Acteurs ayant joué plusieurs personnages dans un même film, triés par nombre de rôles.
    (Utilise $unwind et $group pour compter les rôles dans le tableau 'cast')
    """
    print(f"\n--- Exécution de la Requête 3: Acteurs Multi-rôles ---")

    pipeline = [
        # 1. Décomposer le tableau 'cast' (chaque rôle devient un document temporaire)
        {
            "$unwind": "$cast"
        },
        # 2. Grouper par film et par nom d'acteur pour compter les rôles
        {
            "$group": {
                "_id": {
                    "film_id": "$_id",
                    "actor_name": "$cast.name"
                },
                "movie_title": { "$first": "$title" },
                "role_count": { "$sum": 1 }
            }
        },
        # 3. Filtrer uniquement ceux ayant plus d'un rôle
        {
            "$match": {
                "role_count": { "$gt": 1 }
            }
        },
        # 4. Tri par nombre de rôles décroissant
        {
            "$sort": { "role_count": -1 }
        },
        # 5. Projection
        {
            "$project": {
                "_id": 0,
                "Actor": "$_id.actor_name",
                "Film": "$movie_title",
                "NombreDeRoles": "$role_count"
            }
        }
    ]

    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

@time_query
def query_4_collaborations_director_actor(db: pymongo.database.Database, actor_name: str, time_results: Dict) -> List[Dict]:
    """
    4. Collaborations : Réalisateurs ayant travaillé avec un acteur spécifique, avec le nombre de films ensemble.
    (Simule une sous-requête avec $match, puis $unwind et $group)
    """
    print(f"\n--- Exécution de la Requête 4: Collaborations de '{actor_name}' avec Réalisateurs ---")

    pipeline = [
        # 1. Match: Filtrer les films dans lesquels l'acteur a joué
        {
            "$match": {
                "cast.name": actor_name
            }
        },
        # 2. Décomposer le tableau 'directors'
        {
            "$unwind": "$directors"
        },
        # 3. Grouper par nom du réalisateur
        {
            "$group": {
                "_id": "$directors.name",
                "FilmCount": { "$sum": 1 }
            }
        },
        # 4. Tri par nombre de films décroissant
        {
            "$sort": { "FilmCount": -1 }
        },
        # 5. Projection
        {
            "$project": {
                "_id": 0,
                "DirectorName": "$_id",
                "FilmsEnsemble": "$FilmCount"
            }
        }
    ]

    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

@time_query
def query_5_popular_genres(db: pymongo.database.Database, time_results: Dict) -> List[Dict]:
    """
    5. Genres populaires : Genres ayant une note moyenne > 7.0 et plus de 50 films, triés par note.
    (Équivalent MongoDB de GROUP BY ($group) et HAVING ($match après $group))
    """
    print(f"\n--- Exécution de la Requête 5: Genres Populaires (Note > 7.0, Films > 50) ---")

    pipeline = [
        # 1. Décomposer le tableau 'genre'
        {
            "$unwind": "$genre"
        },
        # 2. Grouper par genre et calculer les statistiques (Note moyenne, nombre de films)
        {
            "$group": {
                "_id": "$genre",
                "AverageRating": { "$avg": "$rating.average" },
                "FilmCount": { "$sum": 1 }
            }
        },
        # 3. Filtrer (Équivalent HAVING)
        {
            "$match": {
                "AverageRating": { "$gt": 7.0 },
                "FilmCount": { "$gt": 50 }
            }
        },
        # 4. Tri par note moyenne décroissante
        {
            "$sort": { "AverageRating": -1 }
        },
        # 5. Projection
        {
            "$project": {
                "_id": 0,
                "Genre": "$_id",
                "NoteMoyenne": { "$round": ["$AverageRating", 2] },
                "NombreDeFilms": "$FilmCount"
            }
        }
    ]

    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

@time_query
def query_6_actor_career_evolution(db: pymongo.database.Database, actor_name: str, time_results: Dict) -> List[Dict]:
    """
    6. Évolution de carrière : Pour un acteur donné, nombre de films par décennie avec note moyenne.
    (Utilise $addFields pour créer la décennie, simulant un calcul dans un CTE)
    """
    print(f"\n--- Exécution de la Requête 6: Évolution de carrière de '{actor_name}' ---")

    pipeline = [
        # 1. Match: Filtrer les films de l'acteur
        {
            "$match": {
                "cast.name": actor_name,
                "year": { "$ne": None, "$gt": 0 }, # Assurer une année valide
                "rating.average": { "$exists": True }
            }
        },
        # 2. Ajouter le champ 'decade' (calcul : (année / 10) * 10)
        {
            "$addFields": {
                "decade": {
                    "$multiply": [
                        10,
                        { "$toInt": { "$floor": { "$divide": ["$year", 10] } } }
                    ]
                }
            }
        },
        # 3. Grouper par décennie
        {
            "$group": {
                "_id": "$decade",
                "TotalFilms": { "$sum": 1 },
                "AverageRating": { "$avg": "$rating.average" }
            }
        },
        # 4. Tri par décennie
        {
            "$sort": { "_id": 1 }
        },
        # 5. Projection
        {
            "$project": {
                "_id": 0,
                "Decennie": { "$concat": [{ "$toString": "$_id" }, "s"] },
                "NombreFilms": "$TotalFilms",
                "NoteMoyenne": { "$round": ["$AverageRating", 2] }
            }
        }
    ]

    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

@time_query
def query_7_ranking_by_genre(db: pymongo.database.Database, top_n: int = 3, time_results: Dict = None) -> List[Dict]:
    """
    7. Classement par genre : Pour chaque genre, les 3 meilleurs films avec leur rang.
    (Utilise l'opérateur de fenêtre $setWindowFields avec $rank, équivalent de RANK() SQL)
    """
    print(f"\n--- Exécution de la Requête 7: Classement des {top_n} Meilleurs Films par Genre ---")

    pipeline = [
        # 1. Décomposer les genres
        {
            "$unwind": "$genre"
        },
        # 2. Filtrer pour s'assurer que les notes existent
        {
            "$match": {
                "rating.average": { "$exists": True, "$ne": None, "$gt": 0 }
            }
        },
        # 3. Calculer le rang (RANK())
        {
            "$setWindowFields": {
                "partitionBy": "$genre", # OVER (PARTITION BY genre)
                "sortBy": { "rating.average": -1 },
                "output": {
                    "rank_by_genre": { "$rank": {} } # RANK()
                }
            }
        },
        # 4. Filtrer pour ne garder que le Top N
        {
            "$match": { "rank_by_genre": { "$lte": top_n } }
        },
        # 5. Tri final pour l'affichage
        {
            "$sort": {
                "genre": 1,
                "rank_by_genre": 1
            }
        },
        # 6. Projection
        {
            "$project": {
                "_id": 0,
                "Rang": "$rank_by_genre",
                "Genre": "$genre",
                "Titre": "$title",
                "Annee": "$year",
                "Note": "$rating.average"
            }
        }
    ]

    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

@time_query
def query_8_breakout_career(db: pymongo.database.Database, time_results: Dict) -> List[Dict]:
    """
    8. Carrière propulsée : Acteurs ayant percé grâce à un film (avant : films < 200k votes, après : films > 200k votes).
    (Agrégation complexe : trouve le premier film à succès (breakout) d'un acteur en triant par année)
    """
    print(f"\n--- Exécution de la Requête 8: Carrière Propulsée (Breakout) ---")

    BREAKOUT_VOTES = 200000

    pipeline = [
        # 1. Décomposer les acteurs
        {
            "$unwind": "$cast"
        },
        # 2. Grouper par acteur et collecter tous leurs films triés par année
        {
            "$group": {
                "_id": "$cast.name",
                "films_tries": {
                    "$push": {
                        "title": "$title",
                        "year": "$year",
                        "votes": "$rating.votes"
                    }
                }
            }
        },
        # 3. Trier les films dans l'ordre chronologique
        {
            "$addFields": {
                "films_tries": {
                    "$sortArray": {
                        "input": "$films_tries",
                        "sortBy": { "year": 1 }
                    }
                }
            }
        },
        # 4. Filtrer la liste des films pour trouver le premier film au-dessus du seuil (le "breakout")
        {
            "$addFields": {
                "breakout_film": {
                    "$filter": {
                        "input": "$films_tries",
                        "as": "f",
                        "cond": { "$gte": ["$$f.votes", BREAKOUT_VOTES] }
                    }
                }
            }
        },
        # 5. Filtrer les acteurs qui ont au moins un film 'breakout'
        {
            "$match": {
                "breakout_film": { "$ne": [] }
            }
        },
        # 6. Projection et extraction du premier film 'breakout'
        {
            "$project": {
                "_id": 0,
                "Acteur": "$_id",
                "FilmBreakout": { "$arrayElemAt": ["$breakout_film.title", 0] },
                "AnneeBreakout": { "$arrayElemAt": ["$breakout_film.year", 0] },
                "VotesBreakout": { "$arrayElemAt": ["$breakout_film.votes", 0] }
            }
        },
        # 7. Tri par année du breakout
        {
            "$sort": { "AnneeBreakout": 1 }
        },
        {
            "$limit": 10
        }
    ]

    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

@time_query
def query_9_free_query_complex_match(db: pymongo.database.Database, time_results: Dict) -> List[Dict]:
    """
    9. Requête libre : Trouver les films réalisés par des personnes qui ont également été scénaristes ET acteurs.
    Note: Puisque la table est dénormalisée, nous utilisons des comparaisons de champs dans le pipeline
    pour simuler la condition "Réalisateur = Scénariste = Acteur" plutôt que d'utiliser de vrais $lookups.
    """
    print(f"\n--- Exécution de la Requête 9: Films de Réalisateurs qui sont aussi Scénaristes ET Acteurs (Note > 8.0) ---")

    pipeline = [
        # 1. Match: Filtrer les films avec une bonne note et un réalisateur/scénariste/acteur
        {
            "$match": {
                "rating.average": { "$gt": 8.0 },
                "directors": { "$exists": True, "$ne": [] },
                "writers": { "$exists": True, "$ne": [] },
                "cast": { "$exists": True, "$ne": [] }
            }
        },
        # 2. Unwind les réalisateurs
        {
            "$unwind": "$directors"
        },
        # 3. Filtrer (Simule 1ère jointure): Réalisateur est dans la liste des Scénaristes
        {
            "$match": {
                "writers.name": "$directors.name"
            }
        },
        # 4. Filtrer (Simule 2ème jointure): Réalisateur/Scénariste est dans la liste des Acteurs
        {
            "$match": {
                "cast.name": "$directors.name"
            }
        },
        # 5. Grouper pour obtenir les films uniques correspondant au critère
        {
            "$group": {
                "_id": "$_id",
                "title": { "$first": "$title" },
                "year": { "$first": "$year" },
                "director": { "$first": "$directors.name" },
                "rating": { "$first": "$rating.average" }
            }
        },
        # 6. Tri et Limite
        {
            "$sort": { "rating": -1 }
        },
        {
            "$limit": 10
        },
        # 7. Projection
        {
            "$project": {
                "_id": 0,
                "Titre": "$title",
                "Annee": "$year",
                "Real_Scen_Acteur": "$director",
                "Note": "$rating"
            }
        }
    ]

    results = list(db[TARGET_COLLECTION].aggregate(pipeline))
    return results

def main_queries():
    db = get_db_connection(MONGO_URI, DB_NAME)

    query_execution_times: Dict[str, Dict] = {
        'query_1_filmography_by_actor': {'sql_time': 18.003103733062744},
        'query_2_top_n_movies_by_genre': {'sql_time': 0.34579944610595703},
        'query_3_multi_role_actors': {'sql_time': 20.788132905960083},
        'query_4_collaborations_director_actor': {'sql_time': 33.144147634506226},
        'query_5_popular_genres': {'sql_time': 1.5411641597747803},
        'query_6_actor_career_evolution': {'sql_time': 15.812682151794434},
        'query_7_ranking_by_genre': {'sql_time': 0.9258627891540527},
        'query_8_breakout_career': {'sql_time': 42.09673237800598},
        'query_9_free_query_complex_match': {'sql_time': 7.226547002792358}
    }

    if db is None:
        return

    # --- EXÉCUTION DES FONCTIONS DE REQUÊTES ---

    ACTOR = "Tom Hanks"
    GENRE = "Drama"

    print("\n" + "="*80)
    print(f"--- DÉBUT DE L'EXÉCUTION DES 9 REQUÊTES MONGO COMPLEXES SUR '{TARGET_COLLECTION}' ---")
    print("="*80)

    # R1: Filmographie d'un acteur
    r1_results = query_1_filmography_by_actor(db, ACTOR, time_results=query_execution_times)
    print(f"Films trouvés pour {ACTOR} (Top 5) : {len(r1_results)}")
    print(json.dumps(r1_results[:5], indent=2))

    # R2: Top N films
    r2_results = query_2_top_n_movies_by_genre(db, GENRE, 2000, 2010, 5, time_results=query_execution_times)
    print(f"Top 5 {GENRE} (2000-2010) :")
    print(json.dumps(r2_results, indent=2))

    # R3: Acteurs multi-rôles
    r3_results = query_3_multi_role_actors(db, time_results=query_execution_times)
    print(f"Acteurs ayant des multi-rôles (Top 5) :")
    print(json.dumps(r3_results[:5], indent=2))

    # R4: Collaborations Réalisateur/Acteur
    r4_results = query_4_collaborations_director_actor(db, ACTOR, time_results=query_execution_times)
    print(f"Collaborations de {ACTOR} (Top 5) :")
    print(json.dumps(r4_results[:5], indent=2))

    # R5: Genres populaires
    r5_results = query_5_popular_genres(db, time_results=query_execution_times)
    print(f"Genres Populaires (Note > 7.0, Films > 50) : {len(r5_results)}")
    print(json.dumps(r5_results[:5], indent=2))

    # R6: Évolution de carrière
    r6_results = query_6_actor_career_evolution(db, ACTOR, time_results=query_execution_times)
    print(f"Évolution de carrière de {ACTOR} :")
    print(json.dumps(r6_results, indent=2))

    # R7: Classement par genre (Top 3)
    r7_results = query_7_ranking_by_genre(db, top_n=3, time_results=query_execution_times)
    print(f"Top 3 Films par Genre (Exemple de quelques résultats) : {len(r7_results)}")
    print(json.dumps(r7_results[:10], indent=2))

    # R8: Carrière propulsée
    r8_results = query_8_breakout_career(db, time_results=query_execution_times)
    print(f"Top 10 Carrières Propulsées :")
    print(json.dumps(r8_results, indent=2))

    # R9: Requête libre (Complexité simulée)
    r9_results = query_9_free_query_complex_match(db, time_results=query_execution_times)
    print(f"Films de Réal/Scén/Acteur (Top 10) : {len(r9_results)}")
    print(json.dumps(r9_results, indent=2))


    # --- AFFICHAGE DES TEMPS D'EXÉCUTION ---
    print("\n" + "="*76)
    print("        --- TEMPS D'EXÉCUTION DES REQUÊTES MONGO (secondes) ---")
    print("="*76)
    print("| Requête                                  | SQL      | Mongo Denormalized |")

    for query_name, res in query_execution_times.items():
        print(f"| {query_name:<40} | {'{:6.3f}'.format(res['sql_time'])} s |            {'{:6.3f}'.format(res['mongoNew'])} s|")
    print("="*76)

    # Fermer la connexion
    if db.client:
        db.client.close()

if __name__ == "__main__":
    main_queries()