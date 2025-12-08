# scripts/phase1_sqlite/create_schema.py

import sqlite3
import os

DB_NAME = 'imdb.db'
DB_PATH = os.path.join('data', DB_NAME)
SCHEMA = """
-- ------------------------------------------------
-- Entité principale : MOVIE
-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS Movie (
    mid TEXT PRIMARY KEY,    -- tconst de IMDB
    title TEXT NOT NULL,
    startYear INTEGER,
    runtimeMinutes INTEGER
);

-- ------------------------------------------------
-- Entité principale : PERSON
-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS Person (
    pid TEXT PRIMARY KEY,   -- nconst de IMDB
    name TEXT NOT NULL,
    birthYear INTEGER,
    deathYear INTEGER
);

-- ------------------------------------------------
-- Entité : RATING (dépend de Movie)
-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS Rating (
    mid TEXT PRIMARY KEY,    -- Clé primaire et étrangère
    averageRating REAL,
    numVotes INTEGER,
    FOREIGN KEY(mid) REFERENCES Movie(mid)
);

-- ------------------------------------------------
-- Table de lien : MOVIE_GENRE (N-N entre Movie et Genre)
-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS MovieGenre (
    mid TEXT,
    genre TEXT NOT NULL,
    PRIMARY KEY(mid, genre),
    FOREIGN KEY(mid) REFERENCES Movie(mid)
);

-- ------------------------------------------------
-- Table de lien : PRINCIPAL (Acteurs/Rôles - N-N)
-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS Principal (
    mid TEXT,
    pid TEXT,
    ordering INTEGER,
    category TEXT, -- Ex: actor, actress, director, writer
    job TEXT,      -- Job spécifique
    PRIMARY KEY(mid, pid, ordering),
    FOREIGN KEY(mid) REFERENCES Movie(mid),
    FOREIGN KEY(pid) REFERENCES Person(pid)
);

-- ------------------------------------------------
-- Table de lien : CHARACTER (Détail des personnages joués)
-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS Character (
    mid TEXT,
    pid TEXT,
    character TEXT,
    PRIMARY KEY(mid, pid, character),
    FOREIGN KEY(mid) REFERENCES Movie(mid),
    FOREIGN KEY(pid) REFERENCES Person(pid)
);

-- NOTE: Les tables MovieDirector et MovieWriter peuvent être modélisées comme des sous-ensembles
-- de la table Principal si la colonne 'category' est suffisante, ou créées séparément
-- si les fichiers director.csv et writers.csv apportent des colonnes supplémentaires.
-- Dans cet exemple, nous nous basons sur Principal pour simplifier.
"""

def create_database_schema():
    """Crée la base de données SQLite et les tables définies dans le SCHEMA."""

    # S'assurer que le dossier data existe
    if not os.path.exists('data'):
        os.makedirs('data')

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Activer la vérification des clés étrangères
        cursor.execute('PRAGMA foreign_keys = ON;')

        # Exécuter les commandes de création de tables
        cursor.executescript(SCHEMA)

        conn.commit()
        print(f"✅ Schéma SQLite créé avec succès dans {DB_NAME}.")

    except sqlite3.Error as e:
        print(f"❌ Une erreur SQLite s'est produite: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    create_database_schema()