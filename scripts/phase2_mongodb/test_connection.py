from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)

client.admin.command("ping")
print("✅ Connexion MongoDB OK")

db = client["imdb"]
db.test.insert_one({"phase": "T2.1", "status": "ok"})
print("✅ Insertion OK")

client.close()
