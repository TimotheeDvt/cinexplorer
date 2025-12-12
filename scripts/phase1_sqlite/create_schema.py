import sqlite3
import os

DB_NAME = './data/imdb.db'

def create_schema():
    print(f"Création de la base de données : {DB_NAME}...")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Activation des contraintes de clés étrangères (Indispensable pour SQLite)
    cursor.execute("PRAGMA foreign_keys = ON;")

    # =========================================================================
    # 1. TABLES PRINCIPALES (ENTITÉS)
    # Ces tables doivent exister pour que MID et PID puissent être référencés
    # =========================================================================

    # Table des Films (Utilise MID comme clé primaire)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Movies (
        MID varchar(50) PRIMARY KEY,
        titletype VARCHAR(50),
        primaryTitle VARCHAR(255),
        originalTitle VARCHAR(255),
        isAdult BOOLEAN,
        startYear INTEGER,
        endYear INTEGER,
        runtimeMinutes INTEGER
    );
    """)
    print("✅ Table 'Movies' créée.")

    # Table des Personnes (Utilise PID comme clé primaire)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Persons (
        PID varchar(50) PRIMARY KEY,
        primaryName VARCHAR(255),
        birthYear INTEGER,
        deathYear INTEGER
    );
    """)
    print("✅ Table 'Persons' créée.")

    # =========================================================================
    # 2. TABLES DE RELATIONS (VOS FICHIERS)
    # =========================================================================

    # --- TABLE CHARACTERS ---
    # Lien N-M entre Movies et Persons (Un acteur joue un personnage dans un film)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Characters (
        MID varchar(50) NOT NULL,
        PID varchar(50) NOT NULL,
        name varchar(255) NOT NULL,
        PRIMARY KEY (MID, PID, name),
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE,
        FOREIGN KEY (PID) REFERENCES Persons(PID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Characters' créée.")

    # --- TABLE DIRECTORS ---
    # Lien N-M entre Movies et Persons (Un réalisateur dirige un film)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Directors (
        MID varchar(50) NOT NULL,
        PID varchar(50) NOT NULL,
        PRIMARY KEY (MID, PID),
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE,
        FOREIGN KEY (PID) REFERENCES Persons(PID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Directors' créée.")

    # --- TABLE EPISODES ---
    # Auto-jointure : Un épisode (MID) appartient à une Série (parentMID)
    # Les deux sont des références à la table Movies
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Episodes (
        MID varchar(50) PRIMARY KEY,
        parentMID varchar(50) NOT NULL,
        seasonNumber INTEGER,
        episodeNumber INTEGER,
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE,
        FOREIGN KEY (parentMID) REFERENCES Movies(MID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Episodes' créée.")

# --- TABLE Genres ---
    # Lien N-M entre Movies et Persons (Un film a un ou pls genres)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Genres (
        MID varchar(50) NOT NULL,
        genre varchar(50) NOT NULL,
        PRIMARY KEY (MID, genre),
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Genres' créée.")

# --- TABLE KnowForMovies ---
    # Lien N-M entre Movies et Persons (Une personne est connue pour un ou pls films)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS KnownForMovies (
        PID varchar(50) NOT NULL,
        MID varchar(50) NOT NULL,
        PRIMARY KEY (PID, MID),
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE,
        FOREIGN KEY (PID) REFERENCES Persons(PID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'KnownForMovies' créée.")

# --- TABLE Principals ---
    # Lien N-M entre Movies et Persons (Une personne a un rôle dans un film)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Principals (
        MID varchar(50) NOT NULL,
        ordering INTEGER NOT NULL,
        PID varchar(50) NOT NULL,
        category varchar(50),
        job varchar(255),
        PRIMARY KEY (MID, PID),
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE,
        FOREIGN KEY (PID) REFERENCES Persons(PID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Principals' créée.")

# --- TABLE Professions ---
    # Lien N-M entre Movies et Persons (Une personne a une ou pls professions)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Professions (
        PID varchar(50) NOT NULL,
        jobname varchar(100) NOT NULL,
        PRIMARY KEY (PID, jobname),
        FOREIGN KEY (PID) REFERENCES Persons(PID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Professions' créée.")

# --- TABLE Ratings ---
    # Lien N-M entre Movies et Persons (Un film a une note et un nb de votes)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Ratings (
        MID varchar(50) NOT NULL,
        averageRating FLOAT NOT NULL,
        numVotes INTEGER NOT NULL,
        PRIMARY KEY (MID),
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Ratings' créée.")


# --- TABLE Titles ---
    # Lien N-M entre Movies et Persons (Un film a un ou pls titres (selon le pays))
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Titles (
        MID varchar(50) NOT NULL,
        ordering INTEGER NOT NULL,
        title VARCHAR(255) NOT NULL,
        region VARCHAR(2),
        language VARCHAR(50),
        types VARCHAR(50),
        attributes VARCHAR(255),
        isOriginalTitle BOOLEAN,
        PRIMARY KEY (MID, ordering),
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Titles' créée.")

# --- TABLE Writers ---
    # Lien N-M entre Movies et Persons (Un film est écrit par une ou pls personnes)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Writers (
        MID varchar(50) NOT NULL,
        PID varchar(50) NOT NULL,
        PRIMARY KEY (MID, PID),
        FOREIGN KEY (MID) REFERENCES Movies(MID) ON DELETE CASCADE,
        FOREIGN KEY (PID) REFERENCES Persons(PID) ON DELETE CASCADE
    );
    """)
    print("✅ Table 'Writers' créée.")

    conn.commit()
    conn.close()
    print("\n🚀 Schéma terminé avec succès.")

if __name__ == "__main__":
    # Nettoyage pour repartir à zéro
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        print(f"Fichier '{DB_NAME}' supprimé pour réinitialisation.")

    create_schema()