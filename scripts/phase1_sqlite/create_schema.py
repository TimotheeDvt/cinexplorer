import sqlite3

# Chemin vers la base de données SQLite
DB_PATH = "data/imdb.db"

def create_schema(conn: sqlite3.Connection):
    """
    Crée toutes les tables de la base de données IMDB dans l'ordre de dépendance.
    Les tables parentes sont créées en premier.
    """
    cursor = conn.cursor()

    # ------------------------------------------------
    # 1. TABLES PARENTES (Pas de FOREIGN KEY sortante)
    # ------------------------------------------------

    # Entité principale : MOVIE (Films)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Movie (
            mid TEXT PRIMARY KEY,     -- tconst de IMDB
            titleType TEXT NOT NULL,
            primaryTitle TEXT NOT NULL,
            originalTitle TEXT NOT NULL,
            isAdult INTEGER,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER
        );
    """)

    # Entité principale : PERSON (Personnes)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Person (
            pid TEXT PRIMARY KEY,   -- nconst de IMDB
            primaryName TEXT NOT NULL,
            birthYear INTEGER,
            deathYear INTEGER
        );
    """)

    # ------------------------------------------------
    # 2. TABLES ENFANTS (Dépendent de Movie et/ou Person)
    # ------------------------------------------------

    # Entité : RATING (dépend de Movie)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Rating (
            mid TEXT PRIMARY KEY,     -- Clé primaire et étrangère
            averageRating REAL,
            numVotes INTEGER,
            FOREIGN KEY(mid) REFERENCES Movie(mid)
        );
    """)

    # Table de lien : PRINCIPAL (Acteurs/Rôles - N-N)
    # C'est la table qui contient la relation de base entre Film et Personne
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Principal (
            mid TEXT,
            ordering INTEGER,
            pid TEXT,
            category TEXT, -- Ex: actor, actress, director, writer
            job TEXT,      -- Job spécifique
            PRIMARY KEY(mid, pid, ordering),
            FOREIGN KEY(mid) REFERENCES Movie(mid),
            FOREIGN KEY(pid) REFERENCES Person(pid)
        );
    """)

    # Table de lien : MOVIE_GENRE (N-N entre Movie et Genre)
    # Renommée en 'Genre' dans votre schéma, mais agit comme une table de lien
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Genre (
            mid TEXT,
            genre TEXT NOT NULL,
            PRIMARY KEY(mid, genre),
            FOREIGN KEY(mid) REFERENCES Movie(mid)
        );
    """)

    # Table de lien : DIRECTOR (dépend de Movie et Person)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Director (
            mid TEXT,
            pid TEXT,
            PRIMARY KEY(mid, pid),
            FOREIGN KEY(mid) REFERENCES Movie(mid),
            FOREIGN KEY(pid) REFERENCES Person(pid)
        );
    """)

    # Table de lien : WRITER (dépend de Movie et Person)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Writer (
            mid TEXT,
            pid TEXT,
            PRIMARY KEY(mid, pid),
            FOREIGN KEY(mid) REFERENCES Movie(mid),
            FOREIGN KEY(pid) REFERENCES Person(pid)
        );
    """)

    # Table de lien : CHARACTER (Détail des personnages joués)
    # Dépend de Movie et Person (via Principal/Principals.csv)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Character (
            mid TEXT,
            pid TEXT,
            name TEXT,
            PRIMARY KEY(mid, pid, name),
            FOREIGN KEY(mid) REFERENCES Movie(mid),
            FOREIGN KEY(pid) REFERENCES Person(pid)
        );
    """)

    # Table de lien : TITLE (Titres alternatifs, dépend de Movie)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Title (
            mid TEXT,
            ordering INTEGER,
            title TEXT,
            region TEXT,
            language TEXT,
            types TEXT,
            attributes TEXT,
            isOriginalTitle INTEGER,
            PRIMARY KEY(mid, title, ordering, region),
            FOREIGN KEY(mid) REFERENCES Movie(mid)
        );
    """)

    # Tables supplémentaires (dépendent de Person/Movie)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS KnownForMovie (
            mid TEXT,
            pid TEXT,
            PRIMARY KEY(mid, pid),
            FOREIGN KEY(mid) REFERENCES Movie(mid),
            FOREIGN KEY(pid) REFERENCES Person(pid)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Profession (
            pid TEXT,
            jobName TEXT,
            PRIMARY KEY(pid, jobName),
            FOREIGN KEY(pid) REFERENCES Person(pid)
        );
    """)

    conn.commit()
    print("✅ Schéma SQLite créé avec succès.")

def main():
    try:
        # Connexion à la base de données. Créera le fichier s'il n'existe pas.
        conn = sqlite3.connect(DB_PATH)
        # Activer l'intégrité référentielle pour les FOREIGN KEY
        conn.execute("PRAGMA foreign_keys = ON")
        create_schema(conn)
    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main()