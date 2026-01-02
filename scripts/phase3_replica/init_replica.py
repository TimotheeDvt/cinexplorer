from pymongo import MongoClient

client = MongoClient("localhost:27017", directConnection=True)

try:
    status = client.admin.command("replSetGetStatus")
    print(f"--- Statut du Replica Set : {status['set']} ---")

    for member in status['members']:
        name = member['name']
        state = member['stateStr']
        uptime = member.get('uptime', 0)
        print(f"Noeud : {name} | État : {state} | Uptime : {uptime}s")

except Exception as e:
    print(f"Erreur lors de la récupération du statut : {e}")