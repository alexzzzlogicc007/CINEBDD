import sqlite3
from pymongo import MongoClient


SQLITE_DB_PATH = "data/imdb.db"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "imdb"


def get_sqlite_tables(conn):
    """Retourne la liste des tables SQLite."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%';
    """)
    return [row[0] for row in cursor.fetchall()]


def fetch_table_data(conn, table_name):
    """Récupère toutes les lignes d'une table SQLite sous forme de dictionnaires."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    documents = []
    for row in rows:
        doc = dict(zip(columns, row))
        documents.append(doc)

    return documents


def main():
    # Connexion SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    # Connexion MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB_NAME]

    tables = get_sqlite_tables(sqlite_conn)
    print(f" Tables SQLite trouvées : {tables}")

    for table in tables:
        print(f"\n Migration de la table : {table}")

        # Extraction SQLite
        documents = fetch_table_data(sqlite_conn, table)
        sqlite_count = len(documents)
        print(f"   - Lignes SQLite : {sqlite_count}")

        # Nettoyage collection MongoDB (sécurité)
        mongo_db[table].drop()

        # Insertion MongoDB
        if documents:
            mongo_db[table].insert_many(documents)

        mongo_count = mongo_db[table].count_documents({})
        print(f"   - Documents MongoDB : {mongo_count}")

        # Vérification
        if sqlite_count == mongo_count:
            print(" Comptage OK")
        else:
            print(" Erreur de comptage")

    sqlite_conn.close()
    mongo_client.close()
    print("\n Migration plate terminée avec succès.")


if __name__ == "__main__":
    main()
