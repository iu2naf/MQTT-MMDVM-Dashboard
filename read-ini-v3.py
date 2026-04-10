import paho.mqtt.client as mqtt
import json
import time
from pathlib import Path

# --- Configuration ---
BROKER = "your_broker_address"
PORT = 1883
USER = "your_username"
PASSWORD = "your_password"
BASE_TOPIC = "devices"
COMMAND_TOPIC = "devices/control/request" # Il topic dove invierai il comando
FILE_LIST_PATH = r"/your/absolute/path/file_list.txt" 

def parse_file_to_dict(file_path_obj):
    extracted_data = {}
    try:
        content = file_path_obj.read_text(encoding="utf-8")
        for line in content.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                extracted_data[key.strip()] = value.strip()
        return extracted_data
    except Exception as e:
        print(f"Errore lettura {file_path_obj.name}: {e}")
        return None

def send_all_data(client):
    """La logica originale di invio, ora chiamata su richiesta."""
    index_file = Path(FILE_LIST_PATH)
    if not index_file.exists():
        print("Errore: File indice non trovato.")
        return

    file_paths = index_file.read_text(encoding="utf-8").splitlines()
    for line in file_paths:
        clean_path = line.strip()
        if not clean_path: continue
        
        data_file = Path(clean_path)
        if data_file.exists() and data_file.is_file():
            subtopic = data_file.stem 
            final_topic = f"{BASE_TOPIC}/{subtopic}"
            payload_dict = parse_file_to_dict(data_file)
            
            if payload_dict:
                payload_dict["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                payload_json = json.dumps(payload_dict, indent=4)
                client.publish(final_topic, payload_json, qos=1, retain=True)
                print(f"Inviato aggiornamento su: {final_topic}")

def on_connect(client, userdata, flags, rc, properties=None):
    """Si attiva quando il bridge si connette al broker."""
    if rc == 0:
        print(f"Connesso! In ascolto su: {COMMAND_TOPIC}")
        # Sottoscrizione al topic dei comandi
        client.subscribe(COMMAND_TOPIC)
    else:
        print(f"Errore connessione, codice: {rc}")

def on_message(client, userdata, msg):
    """Si attiva quando arriva un messaggio (il comando)."""
    payload = msg.payload.decode().lower()
    print(f"Comando ricevuto su {msg.topic}: {payload}")
    
    # Se riceve "update" o "send", scatena l'invio dei file
    if payload in ["update", "send", "get_data"]:
        print("Eseguo scansione file e invio dati...")
        send_all_data(client)

def start_service():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USER, PASSWORD)
    
    # Assegnazione delle funzioni di callback
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER, PORT, 60)
        # loop_forever blocca lo script e lo tiene in ascolto infinito
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nServizio arrestato dall'utente.")
    except Exception as e:
        print(f"Errore: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    start_service()