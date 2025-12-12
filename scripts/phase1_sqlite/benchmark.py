import sqlite3

DB_NAME = './data/imdb.db'

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# =======================================================================
# 1. Index sur les colonnes de filtre et de tri principales
# Ces index améliorent les requêtes de recherche et de classement globales.
# =======================================================================

# Index sur les Noms de Personnes (pour les recherches avec LIKE '%Nom%')
cursor.execute("CREATE INDEX IF NOT EXISTS idx_persons_primaryName ON Persons (primaryName);")
print("✅ Index 'idx_persons_primaryName' créé.")

# Index sur l'Année de Début des Films (pour les filtres temporels et les GROUP BY par décennie)
cursor.execute("CREATE INDEX IF NOT EXISTS idx_movies_startYear ON Movies (startYear);")
print("✅ Index 'idx_movies_startYear' créé.")

# Index sur les Notes et les Votes (pour les classements globaux et les filtres de Ratings)
# L'ordre DESC est important pour les tris sur les notes.
cursor.execute("CREATE INDEX IF NOT EXISTS idx_ratings_rating_votes ON Ratings (averageRating DESC, numVotes DESC);")
print("✅ Index 'idx_ratings_rating_votes' créé.")

# =======================================================================
# 2. Index de Clés Étrangères Individuelles (Pour les jointures)
# Ces index sont essentiels pour accélérer les opérations de JOINTURE.
# =======================================================================

# Index sur les Clés Étrangères Personnes (PID) dans les tables de relation
cursor.execute("CREATE INDEX IF NOT EXISTS idx_characters_pid ON Characters (PID);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_directors_pid ON Directors (PID);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_knownformovies_pid ON KnownForMovies (PID);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_principals_pid ON Principals (PID);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_professions_pid ON Professions (PID);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_writers_pid ON Writers (PID);")

# Index sur les Clés Étrangères Films (MID) dans les tables de relation
cursor.execute("CREATE INDEX IF NOT EXISTS idx_principals_mid ON Principals (MID);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_characters_mid ON Characters (MID);") # Crucial pour Filmography/Career_Evol

print("✅ Index sur 'PID' et 'MID' dans les tables de relation créés.")


# =======================================================================
# 3. Index Composites pour optimiser GROUP BY, Fenêtrage et Jointures complexes
# Ces index servent des requêtes très spécifiques qui impliquent plusieurs colonnes.
# =======================================================================

# 3.1. Pour Top_N_Films et Rank_by_Genre (Filtre sur genre et Join sur MID)
# L'ordre (genre, MID) permet de filtrer rapidement par genre avant de joindre.
cursor.execute("CREATE INDEX IF NOT EXISTS idx_genres_genre_mid ON Genres (genre, MID);")
print("✅ Index composite 'idx_genres_genre_mid' créé.")

# 3.2. Pour Multi_Roles (Group By PID, MID)
# Permet une recherche rapide des paires (Personne, Film) pour le COUNT.
cursor.execute("CREATE INDEX IF NOT EXISTS idx_characters_pid_mid ON Characters (PID, MID);")
print("✅ Index composite 'idx_characters_pid_mid' créé.")

# 3.3. Pour Collaborations et Career_Booster (Group/Filter par PID, et Join sur MID)
# Aide aux jointures impliquant les rôles principaux.
cursor.execute("CREATE INDEX IF NOT EXISTS idx_principals_pid_mid ON Principals (PID, MID);")
print("✅ Index composite 'idx_principals_pid_mid' créé.")


conn.commit()
conn.close()
print("\n🚀 Création d'index terminée avec succès.")