import paho.mqtt.client as mqtt
import json
import time
import threading
import os
import urllib.request
import datetime

# --- CONFIG ---
MQTT_CONFIG = {
    "broker": os.environ.get("MQTT_BROKER"),
    "port": int(os.environ.get("MQTT_PORT", 1883)),
    "topic": os.environ.get("MQTT_TOPIC", "mmdvm/+/json"),
    "user": os.environ.get("MQTT_USER"),
    "pass": os.environ.get("MQTT_PASS")
}

calls = []
calls_lock = threading.Lock()
user_map, nxdn_map, callsign_map, tg_map = {}, {}, {}, {}

def format_ber(val):
    if val is None or val == "": return "0%"
    try:
        
        if isinstance(val, str) and "%" in val:
            val = val.replace("%", "")
        f_val = float(val)
        
        if f_val < 0.1 and f_val > 0:
            return f"{f_val:.2f}%"
        return f"{f_val:.1f}%"
    except:
        return str(val)

def download_databases():
    base_dir = "/opt/mmdvm_web"
    if not os.path.exists(base_dir):
        base_dir = "."
    
    urls = {
        "nxdn.csv": "https://radioid.net/static/nxdn.csv",
        "user.csv": "https://radioid.net/static/user.csv",
        "dmrid.dat": "https://radioid.net/static/dmrid.dat"
    }
    
    print(f"DEBUG: Avvio aggiornamento database in {base_dir}...")
    for filename, url in urls.items():
        try:
            path = os.path.join(base_dir, filename)
            urllib.request.urlretrieve(url, path)
            print(f"DEBUG: Scaricato {filename}")
        except Exception as e:
            print(f"Errore download {filename}: {e}")
    load_databases()

def db_scheduler():
    print("DEBUG: Scheduler database avviato (prossimo controllo tra 1 ora)")
    last_update_day = -1
    while True:
        now = datetime.datetime.now()
        
        if now.hour == 4 and now.day != last_update_day:
            download_databases()
            last_update_day = now.day
        time.sleep(3600)

def load_databases():
    global user_map, nxdn_map, callsign_map
    
    for db, filename in [("dmr", "user.csv"), ("nxdn", "nxdn.csv")]:
        paths = [f"/opt/mmdvm_web/{filename}", filename]
        found = False
        for path in paths:
            if os.path.exists(path):
                try:
                    loaded_count = 0
                    with open(path, encoding="utf-8", errors="ignore") as f:
                        
                        first_line = next(f, None)
                        if first_line and not first_line.strip().replace(',', '').isnumeric() and "RADIO_ID" not in first_line:
                            
                            pass
                        
                        
                        f.seek(0)
                        header_skipped = False
                        for line in f:
                            p = line.strip().split(",")
                            if len(p) >= 3:
                                radio_id = p[0].strip()
                                
                                if radio_id == "RADIO_ID":
                                    continue
                                if db == "dmr": 
                                    user_map[radio_id] = (p[1].strip(), p[2].strip())
                                else: 
                                    nxdn_map[radio_id] = (p[1].strip(), p[2].strip())
                                
                                # Popolamento callsign_map per D-STAR/YSF
                                callsign = p[1].strip().upper()
                                if callsign and callsign not in callsign_map:
                                    callsign_map[callsign] = p[2].strip()
                                
                                loaded_count += 1
                    print(f"DEBUG: Caricati {loaded_count} record da {path} ({db})")
                    found = True
                    break
                except Exception as e:
                    print(f"Errore caricamento {path}: {e}")
        if not found:
            print(f"ATTENZIONE: Database {filename} non trovato in {paths}")

    # Caricamento FreeDMR.csv per i TalkGroups
    tg_path = "FreeDMR.csv"
    if os.path.exists(tg_path):
        try:
            loaded_tg = 0
            with open(tg_path, encoding="utf-8", errors="ignore") as f:
                import csv
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 3:
                        tg_id = row[1].strip()
                        tg_name = row[2].strip()
                        tg_map[tg_id] = tg_name
                        loaded_tg += 1
            print(f"DEBUG: Caricati {loaded_tg} record da {tg_path} (TG)")
        except Exception as e:
            print(f"Errore caricamento {tg_path}: {e}")

def on_message(client, userdata, msg):
    try:
        # Debug
        topic = msg.topic
        payload_str = msg.payload.decode("utf-8", errors="ignore")
        print(f"DEBUG: Messaggio ricevuto sul topic '{topic}': {payload_str[:150]}...")
        
        raw_data = json.loads(payload_str)
        mode = list(raw_data.keys())[0]
        data = raw_data[mode]
        action = data.get("action")
        slot = data.get("slot", "-")
        now_ts = time.time()

        if action == "start":
            
            topic_parts = topic.split('/')
            node_name = topic_parts[1] if len(topic_parts) >= 2 else "N/A"

            
            src_id_raw = str(data.get("source_id", ""))
            src_call_raw = data.get("source_cs", "")
            
            # Forzatura UID per YSF/D-STAR o se manca ID numerico
            uid = src_call_raw if (mode.upper() in ["YSF", "D-STAR"] or not src_id_raw) else src_id_raw
            
            with calls_lock:
                if any(c["id_raw"] == uid and c["TIME"] == "" and (now_ts - c["start_ts"]) < 2 for c in calls):
                    return

            # Risoluzione nome
            callsign = src_call_raw.upper().strip()
            name = f"{mode} User"
            
            # Pulisce il nominativo per la ricerca (rimuove suffissi come -RPT, -G, ecc.)
            lookup_call = callsign
            for sfx in ["-RPT", "-G", "-L"]:
                if lookup_call.endswith(sfx):
                    lookup_call = lookup_call[:-len(sfx)]
                    break
            
            if mode.upper() in ["YSF", "D-STAR"]:
                name = callsign_map.get(lookup_call, f"{mode} User")
                # Se non trovato per nominativo, prova per ID (caso cross-mode)
                if name == f"{mode} User" and src_id_raw in user_map:
                    callsign_db, name_db = user_map[src_id_raw]
                    callsign, name = callsign_db, name_db
            else:
                db = nxdn_map if mode == "NXDN" else user_map
                callsign_db, first_name = db.get(src_id_raw, (src_id_raw, "Unknown"))
                callsign, name = callsign_db, first_name

            # Aggiunto supporto a 'reflector' e 'destination_cs' per YSF/D-STAR
            tg_id = data.get("reflector") or data.get("destination_id") or data.get("dg-id") or data.get("destination_cs") or "N/A"
            tg_id = str(tg_id).strip()
            tg_label = tg_map.get(tg_id, "")
            target = f"{tg_id} ({tg_label})" if tg_label else tg_id

            with calls_lock:
                calls.append({
                    "FROM": data.get("source", "NET").upper(),
                    "id_raw": uid,
                    "ID": callsign,
                    "NAME": name,
                    "TG": target,
                    "MODE": mode,
                    "SLOT": slot,
                    "NODO": node_name,
                    "BER": format_ber(data.get("ber")),
                    "DATA": time.strftime("%d-%m-%Y"),
                    "ORARIO": time.strftime("%H:%M:%S"),
                    "TIME": "",
                    "start_ts": now_ts
                })
                if len(calls) > 40: calls.pop(0)

        elif action in ["end", "lost", "watchdog", "timeout"]:
            
            with calls_lock:
                for c in reversed(calls):
                    if c["MODE"] == mode and c["TIME"] == "":
                        if mode != "DMR" or c["SLOT"] == slot:
                            json_dur = data.get("duration")
                            try:
                                val_dur = round(float(json_dur), 1) if json_dur is not None else round(now_ts - c["start_ts"], 1)
                            except (ValueError, TypeError):
                                val_dur = round(now_ts - c["start_ts"], 1)
                            c["TIME"] = val_dur
                            if action == "lost": c["TIME"] = f"{c['TIME']}!" 
                            
                            if "ber" in data:
                                c["BER"] = format_ber(data["ber"])
                            break
        
        else:
            
            with calls_lock:
                for c in reversed(calls):
                    if c["MODE"] == mode and c["TIME"] == "":
                        if mode != "DMR" or c["SLOT"] == slot:
                            if "ber" in data:
                                c["BER"] = format_ber(data["ber"])
                            break

    except Exception as e:
        err_mode = mode if 'mode' in locals() else 'JSON'
        print(f"Errore parsing {err_mode}: {e}")

def start_mqtt():
    load_databases()
    
    sched_t = threading.Thread(target=db_scheduler)
    sched_t.daemon = True
    sched_t.start()
    
    client = mqtt.Client()
    client.username_pw_set(MQTT_CONFIG["user"], MQTT_CONFIG["pass"])
    def on_connect(c, u, f, rc):
        if rc == 0:
            print(f"Connesso con successo al broker MQTT. Iscrizione al topic: {MQTT_CONFIG['topic']}")
            c.subscribe(MQTT_CONFIG["topic"])
        else:
            print(f"Connessione MQTT fallita (codice rc={rc})")
            
    client.on_connect = on_connect
    client.on_message = on_message
    while True:
        try:
            client.connect(MQTT_CONFIG["broker"], MQTT_CONFIG["port"], 60)
            break
        except ConnectionRefusedError:
            print("MQTT Broker non ancora pronto. Riprovo tra 5 secondi...")
            time.sleep(5)
        except Exception as e:
            print(f"Errore connessione MQTT: {e}")
            time.sleep(5)
    client.loop_forever()

if __name__ == "__main__":
    start_mqtt()
