import sqlite3
import pandas as pd
import time
from typing import List, Tuple, Any

# Chemin vers la base de données SQLite
DB_PATH = "data/imdb.db"
# Chemin vers le répertoire des fichiers CSV
CSV_DIR = "data/csv/small/"

# Liste ordonnée des tables et des fichiers CSV correspondants
# L'ordre est crucial pour respecter les contraintes de clés étrangères (tables parentes avant enfants)
TABLE_CONFIG: List[Tuple[str, str]] = [
    # Tables parentes
    # ("Person", "persons.csv"),
    # ("Movie", "movies.csv"),
    # ("Genre", "genres.csv"), # Peut-être aussi parent
    # Tables enfants/associatives
    # ("Rating", "ratings.csv"),
    # ("Director", "directors.csv"),
    ("Writer", "writers.csv"),
    # ("Principal", "principals.csv"),
    # ("Character", "characters.csv"),

    # ("Title", "titles.csv"),
    # ("Profession", "professions.csv"),
    # ("KnownForMovie", "knownformovies.csv")
]

def clean_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Nettoie et prépare le DataFrame pour l'insertion.

    NOTE : Cette fonction est simplifiée. Dans un vrai projet, elle devrait
    inclure une gestion plus robuste des NaN, des types de données, et des
    valeurs invalides.
    """

    if table_name == "Writer":
        # Supprimer les lignes en double basées sur la clé primaire (mid, pid, name)
        df.drop_duplicates(subset=['mid', 'pid'], keep='first', inplace=True)

    # Remplacer les NaN (valeurs manquantes) par None pour SQLite
    df = df.where(pd.notnull(df), None)

    # Exemples de nettoyage par table
    if table_name == "movies":
        # S'assurer que 'year' et 'runtime' sont des entiers (ou None)
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        df['runtime'] = pd.to_numeric(df['runtime'], errors='coerce').astype('Int64')


    return df

def import_table(conn: sqlite3.Connection, table_name: str, csv_file: str) -> int:
    """Charge un fichier CSV et insère les données dans la table SQLite."""
    csv_path = CSV_DIR + csv_file
    print(f"\n--- Import de la table {table_name} à partir de {csv_file} ---")
    start_time = time.time()

    try:
        # Lire le CSV avec des colonnes spécifiées si nécessaire, ou auto
        # Utiliser l'encodage 'utf-8' ou 'latin-1' si besoin
        df = pd.read_csv(csv_path, sep=',', encoding='utf-8')

        # Nettoyage des données
        df_cleaned = clean_data(df, table_name)

        # Colonnes à insérer (doivent correspondre aux colonnes du schéma)
        columns = df_cleaned.columns.tolist()
        # Création de la requête INSERT
        placeholders = ', '.join(['?'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # Conversion du DataFrame en liste de tuples pour l'insertion
        data_to_insert: List[Tuple[Any, ...]] = [tuple(row) for row in df_cleaned.values]

        # Insertion dans une transaction pour la performance
        cursor = conn.cursor()
        cursor.executemany(insert_query, data_to_insert)

        rows_inserted = cursor.rowcount
        conn.commit()

        end_time = time.time()

        print(f"✅ {rows_inserted} lignes insérées dans {table_name}.")
        print(f"⏱️ Temps écoulé : {end_time - start_time:.2f} secondes.")
        return rows_inserted

    except FileNotFoundError:
        print(f"❌ Erreur: Le fichier CSV {csv_path} est introuvable.")
        return 0
    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite lors de l'import de {table_name}: {e}")
        conn.rollback() # Annuler la transaction en cas d'erreur
        return 0
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        return 0

def main():
    """Fonction principale pour l'import de toutes les tables."""
    total_rows_imported = 0
    total_start_time = time.time()

    try:
        # Connexion à la base de données
        conn = sqlite3.connect(DB_PATH)
        # Activer l'intégrité référentielle (par défaut dans SQLite, mais bon à vérifier)
        conn.execute("PRAGMA foreign_keys = ON")

        print("🚀 Début de l'importation des données IMDB dans SQLite...")

        for table, csv_file in TABLE_CONFIG:
            rows = import_table(conn, table, csv_file)
            total_rows_imported += rows

    except sqlite3.Error as e:
        print(f"\nFATAL: Impossible de se connecter à la base de données: {e}")
        return
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    total_end_time = time.time()

    ## Afficher les statistiques d'import [cite: 158]
    print("\n--- Statistiques finales ---")
    print(f"Importation terminée. {total_rows_imported} lignes importées au total.")
    print(f"Temps total écoulé: {total_end_time - total_start_time:.2f} secondes.")


if __name__ == "__main__":
    main()