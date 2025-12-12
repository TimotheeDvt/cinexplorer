import sqlite3
import pandas as pd
import time
from typing import List, Tuple, Any

# Chemin vers la base de données SQLite
DB_PATH = "data/imdb.db"
# Chemin vers le répertoire des fichiers CSV
CSV_DIR = "data/csv/imdb-medium/"

# Liste ordonnée des tables et des fichiers CSV correspondants
# L'ordre est crucial : les tables parentes (celles qui ne contiennent que la clé primaire)
# doivent être importées avant les tables enfants/associatives (celles qui contiennent des clés étrangères)
TABLE_CONFIG: List[Tuple[str, str]] = [
    # Tables Parentes (Clés primaires sans références externes initiales)
    ("Persons", "persons.csv"),
    ("Movies", "movies.csv"),
    ("Genres", "genres.csv"),
    # Tables Enfants/Associatives (Contiennent des clés étrangères)
    ("Ratings", "ratings.csv"),
    ("Directors", "directors.csv"),
    ("Writers", "writers.csv"),
    ("Principals", "principals.csv"),
    ("Characters", "characters.csv"),
    ("Titles", "titles.csv"),
    ("Professions", "professions.csv"),
    ("KnownForMovies", "knownformovies.csv"),
    ('Episodes', 'episodes.csv'),
]

def clean_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Nettoie et prépare le DataFrame pour l'insertion dans SQLite.
    Gère les valeurs manquantes et assure les types de données appropriés.
    """

    # Remplacer les NaN (valeurs manquantes) par None pour une insertion correcte dans SQLite
    df = df.where(pd.notnull(df), None)

    # Assurer les types de colonnes spécifiques (exemple pour 'Movies')
    if table_name == "Movies":
        # Convertir 'startYear' et 'runtimeMinutes' en entiers (Int64 supporte les NaN)
        df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce').astype('Int64')
        df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce').astype('Int64')

    return df

def import_table(conn: sqlite3.Connection, table_name: str, csv_file: str) -> int:
    """
    Charge un fichier CSV, nettoie les données et les insère dans la table SQLite correspondante.
    Désactive temporairement les clés étrangères pour accélérer l'importation des grandes tables associatives.
    """
    csv_path = CSV_DIR + csv_file
    print(f"\n--- Import de la table {table_name} à partir de {csv_file} ---")
    start_time = time.time()

    # Tables pour lesquelles on désactive les FK pour une meilleure performance
    foreign_key_tables = ["Ratings", "Directors", "Writers", "Principals", "Characters", "KnownForMovies", "Titles", "Episodes"]
    disable_fk = table_name in foreign_key_tables

    if disable_fk:
        conn.execute("PRAGMA foreign_keys = OFF;")
        print("⚠️ PRAGMA foreign_keys = OFF (Temporairement désactivé pour l'import)")

    try:
        # 1. Lire le CSV
        df = pd.read_csv(csv_path, sep=',', encoding='utf-8')

        # 2. Nettoyer les noms de colonnes : Enlever le format '("nom_colonne")'
        original_cols = df.columns.tolist()
        # Supprime les 2 premiers caractères ('("') et les 3 derniers ('")')
        new_cols = [col[2:-3] for col in original_cols]
        df.columns = new_cols
        # print(f"🧹 Noms de colonnes nettoyés de: {original_cols} à: {new_cols}")

        # 3. Nettoyage et préparation des données
        df_cleaned = clean_data(df, table_name)

        # 4. Préparation de la requête SQL
        columns = df_cleaned.columns.tolist()
        placeholders = ', '.join(['?'] * len(columns))

        # Utiliser 'INSERT OR IGNORE' pour ignorer les lignes qui violent les contraintes
        # d'unicité (PRIMARY KEY ou UNIQUE), ce qui est typique lors de l'import.
        insert_query = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # 5. Conversion du DataFrame en liste de tuples pour l'insertion multiple (executemany)
        data_to_insert: List[Tuple[Any, ...]] = [tuple(row) for row in df_cleaned.values]

        # 6. Insertion des données
        cursor = conn.cursor()
        cursor.executemany(insert_query, data_to_insert)

        rows_processed = cursor.rowcount # Nombre de lignes effectivement insérées
        conn.commit()

        end_time = time.time()

        print(f"✅ {rows_processed} lignes **insérées** (sur {len(data_to_insert)} lignes traitées) dans {table_name}.")
        print(f"⏱️ Temps écoulé : {end_time - start_time:.2f} secondes.")
        return rows_processed

    except FileNotFoundError:
        print(f"❌ Erreur: Le fichier CSV {csv_path} est introuvable.")
        return 0
    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite lors de l'import de {table_name}: {e}")
        conn.rollback()
        return 0
    except Exception as e:
        print(f"❌ Erreur inattendue lors de l'import de {table_name}: {e}")
        return 0
    finally:
        # 7. Réactiver la vérification des clés étrangères quoi qu'il arrive
        if disable_fk:
            conn.execute("PRAGMA foreign_keys = ON;")
            print("✅ PRAGMA foreign_keys = ON (Réactivé)")

def main():
    """Fonction principale pour l'import de toutes les tables."""
    total_rows_imported = 0
    total_start_time = time.time()

    try:
        # Connexion à la base de données
        conn = sqlite3.connect(DB_PATH)
        # S'assurer que les clés étrangères sont ON pour l'ensemble de la session (sauf désactivation locale)
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

    ## Afficher les statistiques d'import
    print("\n--- Statistiques finales ---")
    print(f"Importation terminée. {total_rows_imported} lignes insérées au total.")
    print(f"Temps total écoulé: {total_end_time - total_start_time:.2f} secondes.")


if __name__ == "__main__":
    main()