import subprocess
import time
import os
from pymongo import MongoClient

# Configuration
DB_PATHS = ["./data/mongo/db-1", "./data/mongo/db-2", "./data/mongo/db-3"]
PORTS = [27017, 27018, 27019]
REPL_SET_NAME = "rs0"
processes = {}

def start_node(index):
    port = PORTS[index]
    db_path = DB_PATHS[index]
    if not os.path.exists(db_path): os.makedirs(db_path)
    cmd = ["mongod", "--replSet", REPL_SET_NAME, "--port", str(port), "--dbpath", db_path, "--bind_ip", "localhost"]
    processes[port] = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
    print(f"🚀 Nœud {port} démarré.")

def stop_node(port):
    if port in processes:
        processes[port].terminate()
        processes[port].wait()
        del processes[port]
        print(f"🛑 Nœud {port} arrêté.")

def cleanup():
    print("\n🧹 Nettoyage de Windows...")
    os.system("taskkill /F /IM mongod.exe /T >nul 2>&1")
    print("✨ Système nettoyé.")

# --- DÉBUT DU TEST ---
try:
    print("--- ÉTAPE 1: Initialisation ---")
    for i in range(3): start_node(i)
    
    # Connexion flexible (le driver cherchera le Primary parmi les 3)
    uri = f"mongodb://localhost:{PORTS[0]},{PORTS[1]},{PORTS[2]}/?replicaSet={REPL_SET_NAME}"
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)

    # Auto-initiation si nécessaire
    try:
        print("🔧 Configuration du Replica Set...")
        config = {
            '_id': REPL_SET_NAME,
            'members': [{'_id': i, 'host': f'localhost:{PORTS[i]}'} for i in range(3)]
        }
        client.admin.command("replSetInitiate", config)
        print("✅ Commande d'initialisation envoyée.")
    except Exception as e:
        print("ℹ️ Le Replica Set est probablement déjà initialisé.")

    # ATTENTE CRUCIALE DU PRIMARY
    print("⏳ Attente de l'élection d'un leader...")
    while True:
        try:
            is_master = client.admin.command("isMaster")
            if is_master.get("ismaster"):
                print(f"🌟 Primary détecté sur {is_master.get('me')}")
                break
        except: pass
        time.sleep(1)

    print("\n--- ÉTAPE 2: Test d'écriture ---")
    db = client["IMDB_DB"]
    res = db.test_collection.insert_one({"status": "initial", "time": time.time()})
    print(f"✅ Écriture réussie : {res.inserted_id}")

    print("\n--- ÉTAPE 3 & 4: Panne et Élection ---")
    status = client.admin.command("isMaster")
    primary_port = int(status.get("me").split(":")[1])
    stop_node(primary_port)

    print("Attente du nouveau leader...")
    start_election = time.time()
    # Le driver va mettre un peu de temps à détecter la perte et rafraîchir la topologie
    while True:
        try:
            # On force une commande pour vérifier l'état
            new_status = client.admin.command("isMaster")
            if new_status.get("ismaster"):
                new_p = new_status.get("me")
                print(f"✨ Nouveau Primary élu : {new_p} en {time.time()-start_election:.2f}s")
                break
        except: pass
        time.sleep(1)

    print("\n--- ÉTAPE 5: Vérification lecture ---")
    doc = db.test_collection.find_one({"status": "initial"})
    if doc: print("✅ Données toujours accessibles.")

    print("\n--- ÉTAPE 7: Perte de Quorum (Double panne) ---")
    remaining_ports = list(processes.keys())
    for p in remaining_ports[:2]: stop_node(p)

    try:
        print("Tentative d'écriture (devrait échouer)...")
        db.test_collection.insert_one({"test": "fail"}, timeoutMS=3000)
    except Exception:
        print("✅ Succès : L'écriture a été bloquée (Quorum non atteint).")

except Exception as e:
    print(f"💥 Erreur imprévue : {e}")
finally:
    cleanup()