import pymongo
from typing import List, Dict, Optional
import json
import time

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "IMDB_DB"

# Assurez-vous que ces noms correspondent aux collections créées dans T2.2 (Migration plate)
SOURCE_MOVIE_COLLECTION = "movies"
TARGET_COLLECTION = "movies_complete"
BATCH_SIZE = 1000

# Fonction pour tenter de convertir la chaîne de caractères JSON des personnages en liste Python
def parse_characters(characters_str: str) -> List[str]:
    """Nettoie et convertit la chaîne 'characters' en liste de chaînes."""
    if not characters_str:
        return []

    # Remplacer les quotes simples par des quotes doubles pour un JSON valide
    cleaned_str = characters_str.replace("'", '"')
    try:
        # Tenter le parsing JSON
        return json.loads(cleaned_str)
    except json.JSONDecodeError:
        # En cas d'échec (données malformées), retourner une liste vide
        return []


def create_denormalized_document(db: pymongo.database.Database, movie_MID: str) -> Optional[Dict]:
    """
    Crée un document dénormalisé complet pour un film en agrégeant toutes ses informations
    à partir des collections plates.
    """

    # 1. Récupération des informations de base du film
    movie_doc = db[SOURCE_MOVIE_COLLECTION].find_one({"MID": movie_MID})
    if not movie_doc:
        return None

    # Séparation des genres
    genres_doc = db["genres"].find_one({"MID": movie_MID})
    genre = genres_doc.get("genre") if genres_doc else None

    # 2. Récupération des notes (Ratings)
    rating_doc = db["ratings"].find_one({"MID": movie_MID})
    rating = {
        "average": rating_doc.get("averageRating") if rating_doc else None,
        "votes": rating_doc.get("numVotes") if rating_doc else None
    }

    # 3. Récupération des rôles (Principals) et construction des listes Cast/Directors/Writers
    # Utilisation de find() pour récupérer tous les rôles du film
    principals_cursor = db["principals"].find({"MID": movie_MID})
    principals_list = list(principals_cursor)

    directors = []
    writers = []
    cast = []

    if principals_list:
        # CORRECTION 1: Filtrer les documents principaux pour s'assurer qu'ils ont un 'PID'
        pid_list = [p.get("PID") for p in principals_list if p.get("PID")]

        if pid_list:
            # Optimisation: Récupération des noms de toutes les personnes liées en une seule requête
            person_details_cursor = db["persons"].find({"PID": {"$in": pid_list}}, {"PID": 1, "primaryName": 1, "_id": 0})

            # CORRECTION 2: S'assurer que les documents 'person' ont un 'PID' et un 'primaryName'
            person_map = {p.get("PID"): p.get("primaryName")
                          for p in person_details_cursor if p.get("PID") and p.get("primaryName")}

            for principal in principals_list:
                person_id = principal.get("PID")
                role_category = principal.get("category")

                if not person_id or not role_category:
                    continue

                simplified_info = {
                    "id": person_id,
                    "name": person_map.get(person_id, "Nom inconnu")
                }

                if role_category == "director":
                    directors.append(simplified_info)
                elif role_category in ["writer", "composer"]:
                    writer_info = simplified_info.copy()
                    writer_info["category"] = principal.get("job", "writer")
                    writers.append(writer_info)
                elif role_category in ["actor", "actress"]:
                    cast_info = simplified_info.copy()
                    cast_info["characters"] = parse_characters(principal.get("characters"))
                    cast_info["ordering"] = principal.get("ordering")
                    cast.append(cast_info)

    # 4. Récupération des titres alternatifs (Titles)
    # NOTE: Cette collection peut ne pas exister si vous n'avez pas importé titles.cSV.
    titles_list = []
    if "titles" in db.list_collection_names():
        titles_cursor = db.get_collection("titles").find({"MID": movie_MID}, {"title": 1, "region": 1, "_id": 0})
        titles_list = list(titles_cursor)

    # 5. Construction du document final
    denormalized_doc = {
        "_id": movie_MID,
        "title": movie_doc.get("primaryTitle"),
        "year": movie_doc.get("startYear"),
        "runtime": movie_doc.get("runtimeMinutes"),
        "genre": genre,
        "rating": rating,
        "directors": directors,
        "writers": writers,
        "cast": sorted(cast, key=lambda x: x.get('ordering', 999)),
        "titles": titles_list
    }

    # Nettoyage et retour
    return {k: v for k, v in denormalized_doc.items() if v is not None}


def migrate_data_in_batches(db: pymongo.database.Database) -> None:
    """
    Migre les données de la collection source vers la collection cible en utilisant des lots.
    """

    total_movies = db[SOURCE_MOVIE_COLLECTION].count_documents({})
    movie_MIDs_cursor = db[SOURCE_MOVIE_COLLECTION].find({}, {"_id": 0, "MID": 1}).sort([("MID", 1)])

    print(f"\n--- Démarrage de la migration par lots ---")
    print(f"Total de films à traiter : {total_movies}")
    print(f"Taille des lots : {BATCH_SIZE}")

    movies_to_insert = []
    processed_count = 0
    start_time = time.time()

    for movie_doc in movie_MIDs_cursor:
        MID = movie_doc.get("MID")

        if not MID:
            continue

        denorm_doc = create_denormalized_document(db, MID)

        if denorm_doc:
            movies_to_insert.append(denorm_doc)

        processed_count += 1

        # Exécuter l'insertion lorsque le lot est plein
        if len(movies_to_insert) >= BATCH_SIZE:
            try:
                db[TARGET_COLLECTION].insert_many(movies_to_insert, ordered=False)
                movies_to_insert = []

                elapsed = time.time() - start_time
                progress = (processed_count / total_movies) * 100 if total_movies > 0 else 0

                print(f"Progression : {processed_count}/{total_movies} ({progress:.2f}%) - Temps écoulé : {elapsed:.2f}s")

            except Exception as e:
                print(f"Avertissement : Erreur lors de l'insertion du lot. {e}")
                movies_to_insert = []

    # Insérer le lot final
    if movies_to_insert:
        try:
            db[TARGET_COLLECTION].insert_many(movies_to_insert, ordered=False)
        except Exception as e:
            print(f"Avertissement : Erreur lors de l'insertion du lot final. {e}")

    total_time = time.time() - start_time
    print("-" * 50)
    print(f"✅ Migration terminée. Documents traités : {processed_count}")
    print(f"Temps total écoulé : {total_time:.2f} secondes.")


def main():
    client = None
    try:
        # 1. Initialisation
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        client.admin.command('ping')

        # 2. Création/Vérification des index
        print("🛠️ Création/Vérification des index pour optimiser les jointures (FIND())...")
        # Ces index sont cruciaux pour la performance de la migration par lots
        db["ratings"].create_index("MID")
        db["principals"].create_index("MID")
        db["persons"].create_index("PID")
        if "titles" in db.list_collection_names():
            db["titles"].create_index("MID")
        db[SOURCE_MOVIE_COLLECTION].create_index("MID")
        print("✅ Index créés ou déjà existants.")

        # 3. Suppression de la collection cible précédente
        db[TARGET_COLLECTION].drop()
        print(f"Collection '{TARGET_COLLECTION}' purgée.")

        # 4. Exécution de la migration par lots
        migrate_data_in_batches(db)

        # 5. Affichage d'un aperçu
        sample_doc = db[TARGET_COLLECTION].find_one({"title": "The Shawshank Redemption"})
        if sample_doc:
            print("\n--- Aperçu d'un document dénormalisé (Exemple) ---")
            print(json.dumps(sample_doc, indent=2))
        elif db[TARGET_COLLECTION].count_documents({}) > 0:
            print("\n--- Aperçu d'un document dénormalisé (Aléatoire) ---")
            print(json.dumps(db[TARGET_COLLECTION].find_one(), indent=2))


    except pymongo.errors.ConnectionFailure:
        print(f"❌ Erreur de connexion à MongoDB. Assurez-vous que le serveur est démarré à {MONGO_URI}.")
    except Exception as e:
        print(f"❌ Une erreur inattendue est survenue : {e}")
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()