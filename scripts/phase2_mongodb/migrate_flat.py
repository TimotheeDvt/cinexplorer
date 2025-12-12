# connect to mongoDB
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['IMDB_DB']
# create collections for each table
movies = db['movies']
characters = db['characters']
directors = db['directors']
episodes = db['episodes']
genres = db['genres']
known_for_movies = db['known_for_movies']
persons = db['persons']
principals = db['principals']
ratings = db['ratings']
professions = db['professions']
titles = db['titles']
writers = db['writers']

# migrate flat structure to nested structure from phase 1, sqlite
# connect to sqlite
import sqlite3
conn = sqlite3.connect('./data/imdb.db')
cursor = conn.cursor()

# Extraire les données de SQLite (pas des CSV !)
# Insérer dans MongoDB avec insert_many()

def migrate_table(table_name, collection):
    print(f"Migrating {table_name}...")
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    documents = []
    for row in rows:
        document = {columns[i]: row[i] for i in range(len(columns))}
        documents.append(document)
    if documents:
        collection.insert_many(documents)
    print(f"Migrated {len(documents)} records from {table_name} to MongoDB.")

tables_collections = {
    'Movies': movies,
    'Characters': characters,
    'Directors': directors,
    'Episodes': episodes,
    'Genres': genres,
    'KnownForMovies': known_for_movies,
    'Persons': persons,
    'Principals': principals,
    'Ratings': ratings,
    'Professions': professions,
    'Titles': titles,
    'Writers': writers
}

for table_name, collection in tables_collections.items():
    migrate_table(table_name, collection)

print("Data migration completed.")