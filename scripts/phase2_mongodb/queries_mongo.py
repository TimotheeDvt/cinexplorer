import pymongo
import time
from typing import Tuple, Any, Dict, Callable, List

# Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "IMDB_DB"

def time_query(db: pymongo.database.Database, func: Callable, *args, **kwargs) -> Tuple[Any, float]:
    """
    Exécute une fonction de requête MongoDB, mesure son temps, et retourne le résultat et le temps d'exécution.
    """
    start_time = time.time()
    results = func(db, *args, **kwargs)
    end_time = time.time()
    elapsed_time = end_time - start_time
    return results, elapsed_time


# --------------------------------------------------------------------------
# Fonctions de Requête MongoDB
# --------------------------------------------------------------------------

def query_actor_filmography(db: pymongo.database.Database, actor_name: str) -> List[Dict]:
    """
    Récupère la filmographie complète d'un acteur, triée par année décroissante.
    Utilise $lookup pour joindre Personnes -> Rôles -> Films.
    """
    collection = db["persons"]
    # Utilisation de $regex pour le 'LIKE'
    pipeline = [
        # 1. Trouver l'acteur
        {"$match": {"primaryName": {"$regex": actor_name, "$options": "i"}}},
        # 2. Joindre les rôles (Principals)
        {"$lookup": {
            "from": "principals",
            "localField": "pid",
            "foreignField": "pid",
            "as": "roles"
        }},
        {"$unwind": "$roles"},
        # 3. Joindre les films
        {"$lookup": {
            "from": "movies",
            "localField": "roles.mid",
            "foreignField": "mid",
            "as": "movie_info"
        }},
        {"$unwind": "$movie_info"},
        # 4. Projection et tri
        {"$project": {
            "_id": 0,
            "primaryTitle": "$movie_info.primaryTitle",
            "startYear": "$movie_info.startYear",
            "category": "$roles.category" # Le rôle réel (ex: 'actor', 'director')
        }},
        {"$sort": {"startYear": -1}}
    ]
    return list(collection.aggregate(pipeline))

def query_top_n_films(db: pymongo.database.Database, genre: str, startYear: int, endYear: int, n: int) -> List[Dict]:
    """
    Trouve les N films les mieux notés pour un genre donné et une période spécifique.
    Utilise find() pour la recherche de base et le tri.
    (Si les genres et les notes sont intégrés dans la collection 'movies', c'est simple).
    """
    collection = db["movies"]
    # Assumant que 'genres', 'startYear', et 'averageRating' sont des champs dans la collection 'movies'
    results = collection.find({
        "genres": {"$in": [genre]},
        "startYear": {"$gte": startYear, "$lte": endYear},
        "averageRating": {"$exists": True} # S'assurer que la note existe
    }).sort([("averageRating", -1)]).limit(n)

    # Projection pour correspondre à la sortie SQL
    return list(results.limit(n).project({"primaryTitle": 1, "startYear": 1, "averageRating": 1, "_id": 0}))

def query_actor_multi_roles(db: pymongo.database.Database, n: int) -> List[Dict]:
    """
    Trouve les N acteurs qui jouent plusieurs rôles différents (plusieurs entrées 'principal') dans le même film.
    Utilise $group pour compter les rôles par personne (pid) et film (mid), puis $match.
    """
    collection = db["principals"]
    pipeline = [
        # 1. Grouper par Film (mid) et Personne (pid) pour compter les rôles
        {"$group": {
            "_id": {"pid": "$pid", "mid": "$mid"},
            "role_count": {"$sum": 1}
        }},
        # 2. Filtrer ceux qui ont plus d'un rôle
        {"$match": {"role_count": {"$gt": 1}}},
        # 3. Joindre les informations de la personne et du film
        {"$lookup": {
            "from": "persons",
            "localField": "_id.pid",
            "foreignField": "pid",
            "as": "person_info"
        }},
        {"$unwind": "$person_info"},
        {"$lookup": {
            "from": "movies",
            "localField": "_id.mid",
            "foreignField": "mid",
            "as": "movie_info"
        }},
        {"$unwind": "$movie_info"},
        # 4. Projection et tri
        {"$project": {
            "_id": 0,
            "primaryName": "$person_info.primaryName",
            "primaryTitle": "$movie_info.primaryTitle",
            "role_count": 1
        }},
        {"$sort": {"role_count": -1, "primaryName": 1}},
        {"$limit": n}
    ]
    return list(collection.aggregate(pipeline))

def query_collaborations(db: pymongo.database.Database, actor: str) -> List[Dict]:
    """
    Liste les réalisateurs ayant collaboré avec un acteur spécifique, avec le nombre de films réalisés ensemble.
    Utilise $lookup et $group.
    """
    collection = db["persons"]
    pipeline = [
        # 1. Trouver l'acteur et ses rôles
        {"$match": {"primaryName": actor}},
        {"$lookup": {
            "from": "principals",
            "localField": "pid",
            "foreignField": "pid",
            "as": "actor_roles"
        }},
        {"$unwind": "$actor_roles"},
        # 2. Joindre les réalisateurs de ces films
        {"$lookup": {
            "from": "principals",
            "localField": "actor_roles.mid",
            "foreignField": "mid",
            "as": "director_roles",
            "pipeline": [
                {"$match": {"category": "director"}},
            ]
        }},
        {"$unwind": "$director_roles"},
        # 3. Joindre les informations des réalisateurs
        {"$lookup": {
            "from": "persons",
            "localField": "director_roles.pid",
            "foreignField": "pid",
            "as": "director_info"
        }},
        {"$unwind": "$director_info"},
        # 4. Grouper par réalisateur et compter les collaborations
        {"$group": {
            "_id": "$director_info.pid",
            "Realisateur": {"$first": "$director_info.primaryName"},
            "Nombre_de_Films": {"$sum": 1}
        }},
        # 5. Tri
        {"$sort": {"Nombre_de_Films": -1, "Realisateur": 1}}
    ]
    return list(collection.aggregate(pipeline))

def query_genre_popularity(db: pymongo.database.Database, n: int) -> List[Dict]:
    """
    Trouve les N genres les plus populaires : note moyenne > 7.0 et plus de 50 films.
    Utilise $unwind pour les genres, $group pour l'agrégation.
    """
    collection = db["movies"]
    pipeline = [
        # 1. Séparer les films par genre
        {"$unwind": "$genres"},
        # 2. Grouper par genre pour calculer la note moyenne et le compte
        {"$group": {
            "_id": "$genres",
            "avg_rating": {"$avg": "$averageRating"},
            "film_count": {"$sum": 1}
        }},
        # 3. Filtrer selon les critères de popularité
        {"$match": {
            "avg_rating": {"$gt": 7.0},
            "film_count": {"$gt": 50}
        }},
        # 4. Projection et tri
        {"$project": {
            "_id": 0,
            "genre": "$_id",
            "avg_rating": 1,
            "film_count": 1
        }},
        {"$sort": {"avg_rating": -1}},
        {"$limit": n}
    ]
    return list(collection.aggregate(pipeline))

def query_evolution_career(db: pymongo.database.Database, actor_name: str) -> List[Dict]:
    """
    Analyse l'évolution de la carrière d'un acteur : nombre de films et note moyenne par décennie.
    Utilise $lookup, $group et l'opérateur arithmétique $floor pour calculer la décennie.
    """
    collection = db["persons"]
    pipeline = [
        # 1. Trouver l'acteur
        {"$match": {"primaryName": {"$regex": actor_name, "$options": "i"}}},
        # 2. Joindre les rôles et films
        {"$lookup": {
            "from": "principals",
            "localField": "pid",
            "foreignField": "pid",
            "as": "roles"
        }},
        {"$unwind": "$roles"},
        {"$lookup": {
            "from": "movies",
            "localField": "roles.mid",
            "foreignField": "mid",
            "as": "movie_info"
        }},
        {"$unwind": "$movie_info"},
        # 3. Calculer la décennie (ex: 1995 -> 1990)
        {"$project": {
            "_id": 0,
            "startYear": "$movie_info.startYear",
            "averageRating": "$movie_info.averageRating",
            "decade": {"$multiply": [{"$floor": {"$divide": ["$movie_info.startYear", 10]}}, 10]}
        }},
        # 4. Grouper par décennie pour calculer les stats
        {"$group": {
            "_id": "$decade",
            "film_count": {"$sum": 1},
            "avg_rating": {"$avg": "$averageRating"}
        }},
        # 5. Projection et tri
        {"$project": {
            "_id": 0,
            "decade": "$_id",
            "film_count": 1,
            "avg_rating": 1
        }},
        {"$sort": {"decade": 1}}
    ]
    return list(collection.aggregate(pipeline))

# NOTE: Les fonctions de fenêtre comme RANK() ne sont pas directement disponibles en MongoDB
# et nécessitent des solutions complexes. Pour cette démo, nous ferons une requête de tri simple.
def query_rank_by_genre(db: pymongo.database.Database, genre: str) -> List[Dict]:
    """
    Classe tous les films d'un genre donné en fonction de leur note moyenne (tri simple pour simuler le classement).
    """
    collection = db["movies"]
    # Utilisation de find() avec tri pour obtenir un classement
    results = collection.find({
        "genres": {"$in": [genre]}
    }).sort([("averageRating", -1)])

    # NOTE : La numérotation (rank) doit être faite côté client en Python ou avec une agrégation plus complexe
    # (par exemple en utilisant $setWindowFields introduit dans MongoDB 5.0) si disponible.
    return list(results.project({"primaryTitle": 1, "startYear": 1, "averageRating": 1, "_id": 0}))

def query_carreer_booster(db: pymongo.database.Database, n: int) -> List[Dict]:
    """
    Identifie les N personnes dont la carrière a été le plus 'boostée'.
    Définit un film populaire comme ayant plus de 200 000 votes.
    """
    collection = db["principals"]
    pipeline = [
        # 1. Joindre les informations des films (y compris numVotes)
        {"$lookup": {
            "from": "movies",
            "localField": "mid",
            "foreignField": "mid",
            "as": "movie_info"
        }},
        {"$unwind": "$movie_info"},
        # 2. Grouper par personne (pid) et compter les films avant/après 'break'
        {"$group": {
            "_id": "$pid",
            "before_count": {"$sum": {"$cond": [{"$lt": ["$movie_info.numVotes", 200000]}, 1, 0]}},
            "after_count": {"$sum": {"$cond": [{"$gte": ["$movie_info.numVotes", 200000]}, 1, 0]}}
        }},
        # 3. Filtrer et calculer le ratio
        {"$match": {"before_count": {"$gt": 0}, "after_count": {"$gt": 0}}},
        {"$project": {
            "pid": "$_id",
            "before_count": 1,
            "after_count": 1,
            "career_ratio": {"$divide": ["$after_count", "$before_count"]} # Calcul du ratio
        }},
        # 4. Joindre le nom de la personne
        {"$lookup": {
            "from": "persons",
            "localField": "pid",
            "foreignField": "pid",
            "as": "person_info"
        }},
        {"$unwind": "$person_info"},
        # 5. Projection et tri
        {"$project": {
            "_id": 0,
            "primaryName": "$person_info.primaryName",
            "before_count": 1,
            "after_count": 1,
            "career_ratio": 1
        }},
        {"$sort": {"career_ratio": -1}},
        {"$limit": n}
    ]
    return list(collection.aggregate(pipeline))

def query_free_form(db: pymongo.database.Database) -> List[Dict]:
    """
    Exemple de requête libre : Les 10 rôles de films les mieux notés (note > 8.0).
    """
    collection = db["principals"]
    pipeline = [
        # 1. Joindre les films (pour la note)
        {"$lookup": {
            "from": "movies",
            "localField": "mid",
            "foreignField": "mid",
            "as": "movie_info"
        }},
        {"$unwind": "$movie_info"},
        # 2. Joindre les personnes (pour le nom de l'acteur)
        {"$lookup": {
            "from": "persons",
            "localField": "pid",
            "foreignField": "pid",
            "as": "person_info"
        }},
        {"$unwind": "$person_info"},
        # 3. Filtrer par note
        {"$match": {"movie_info.averageRating": {"$gt": 8.0}}},
        # 4. Tri par note
        {"$sort": {"movie_info.averageRating": -1}},
        {"$limit": 10},
        # 5. Projection
        {"$project": {
            "_id": 0,
            "actor_name": "$person_info.primaryName",
            "movie_title": "$movie_info.primaryTitle",
            "averageRating": "$movie_info.averageRating"
        }}
    ]
    return list(collection.aggregate(pipeline))


# --------------------------------------------------------------------------
# Fonction Principale
# --------------------------------------------------------------------------

def main():
    client = None

    # Ces temps sont des placeholders de la version SQL pour comparer
    benchmark_results: Dict[str, Dict[str, float]] = {
        'Filmography': {'sql_time': 18.003103733062744},
        'Top_N_Films': {'sql_time': 0.34579944610595703},
        'Multi_Roles': {'sql_time': 20.788132905960083},
        'Collaborations': {'sql_time': 33.144147634506226},
        'Genre_Pop': {'sql_time': 1.5411641597747803},
        'Career_Evol': {'sql_time': 15.812682151794434},
        'Rank_by_Genre': {'sql_time': 0.9258627891540527},
        'Career_Booster': {'sql_time': 42.09673237800598},
        'Free_Form': {'sql_time': 7.226547002792358}
    }

    try:
        # Initialisation de la connexion MongoDB
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]

        # Test de connexion
        client.admin.command('ping')
        print(f"✅ Connexion à MongoDB réussie. Base de données: {DB_NAME}")
        print("-" * 50)

        # Exécuter et chronométrer chaque requête
        print("Exécution des requêtes MongoDB...")

        _, t1 = time_query(db, query_actor_filmography, actor_name="Tom Hanks")
        benchmark_results['Filmography']["mongo_time"] = t1

        print(_)

        _, t2 = time_query(db, query_top_n_films, genre="Drama", startYear=1990, endYear=2000, n=5)
        benchmark_results['Top_N_Films']["mongo_time"] = t2

        _, t3 = time_query(db, query_actor_multi_roles, n=10)
        benchmark_results['Multi_Roles']["mongo_time"] = t3

        # Exemple d'affichage
        collaborations_result, t4 = time_query(db, query_collaborations, actor="Tom Hanks")
        benchmark_results['Collaborations']["mongo_time"] = t4
        print(f"Exemple 'Collaborations' (Tom Hanks): {collaborations_result[:3]}...")

        _, t5 = time_query(db, query_genre_popularity, n=10)
        benchmark_results['Genre_Pop']["mongo_time"] = t5

        _, t6 = time_query(db, query_evolution_career, actor_name="Leonardo DiCaprio")
        benchmark_results['Career_Evol']["mongo_time"] = t6

        _, t7 = time_query(db, query_rank_by_genre, genre="Comedy")
        benchmark_results['Rank_by_Genre']["mongo_time"] = t7

        _, t8 = time_query(db, query_carreer_booster, n=10)
        benchmark_results['Career_Booster']["mongo_time"] = t8

        _, t9 = time_query(db, query_free_form)
        benchmark_results['Free_Form']["mongo_time"] = t9

        print("-" * 50)
        print("✅ Benchmark MongoDB terminé.")
        print("\n--- Synthèse des Temps d'Exécution (SQL vs MongoDB) ---")

        # Affichage formaté des résultats
        print("{:<20} {:<10} {:<10}".format('Requête', 'Temps SQL (s)', 'Temps Mongo (s)'))
        print("{:-<20} {:-<10} {:-<10}".format('', '', ''))
        for k, v in benchmark_results.items():
            # Nécessite que 'mongo_time' ait été effectivement mesuré
            if "mongo_time" in v:
                print(f"{k:<20} {v['sql_time']:<10.4f} {v['mongo_time']:<10.4f}")


    except pymongo.errors.ConnectionError as e:
        print(f"❌ Erreur de connexion à MongoDB. Assurez-vous que le serveur est démarré et accessible à {MONGO_URI}.")
        print(f"Détails de l'erreur: {e}")
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()