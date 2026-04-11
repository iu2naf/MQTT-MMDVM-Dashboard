from flask import Flask, jsonify, send_file, Response, stream_with_context, make_response, redirect, url_for
import threading
import mqtt_parser
import os
import json
import queue
import time
from paho.mqtt import client as mqtt_client

app = Flask(__name__, static_folder=".", static_url_path="")

repeaters_messages = {}
repeaters_messages_lock = threading.Lock()
repeaters_last_update = {}
repeaters_client = None
repeaters_client_lock = threading.Lock()

def on_repeaters_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connesso al broker MQTT (Repeaters)")
        client.subscribe("dati/#")
    else:
        print(f"Errore connessione Repeaters: {rc}")

def on_repeaters_message(client, userdata, msg):
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode())
    except:
        payload = msg.payload.decode()

    with repeaters_messages_lock:
        repeaters_messages[topic] = payload
        repeaters_last_update[topic] = time.time()

def repeaters_mqtt_loop():
    global repeaters_client
    with repeaters_client_lock:
        try:
            # Tenta inizializzazione per paho-mqtt >= 2.0.0
            repeaters_client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        except AttributeError:
            # Fallback per versioni precedenti (< 2.0.0)
            repeaters_client = mqtt_client.Client()
        
        repeaters_client.username_pw_set(mqtt_parser.MQTT_CONFIG["user"], mqtt_parser.MQTT_CONFIG["pass"])
        repeaters_client.on_connect = on_repeaters_connect
        repeaters_client.on_message = on_repeaters_message
    
    while True:
        try:
            repeaters_client.connect(mqtt_parser.MQTT_CONFIG["broker"], mqtt_parser.MQTT_CONFIG["port"], 60)
            repeaters_client.loop_forever()
        except Exception as e:
            print(f"Errore MQTT Repeaters: {e}, riconnessione in 5s...")
            time.sleep(5)

def repeaters_hourly_updater():
    """Invia 'update' ogni 3600 secondi sulla queue devices/control/request"""
    while True:
        time.sleep(3600)
        with repeaters_client_lock:
            if repeaters_client and repeaters_client.is_connected():
                try:
                    repeaters_client.publish("devices/control/request", "update")
                    print(f"[{time.strftime('%H:%M:%S')}] Inviato comando 'update' a devices/control/request")
                except Exception as e:
                    print(f"[{time.strftime('%H:%M:%S')}] Errore durante l'invio del comando MQTT: {e}")



@app.route("/")
def index():
    return send_file("index.html")


@app.route("/data")
def data():
    import time
    import subprocess
    try:
        git_hash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
        git_date = subprocess.check_output(["git", "log", "-1", "--format=%cd", "--date=short"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
        version_str = f"{git_hash} - {git_date}"
    except Exception:
        version_str = "1.0.1 β (Static)" # Fallback

    current_calls = mqtt_parser.get_recent_calls(limit=50)
    return jsonify({"server_time": time.time(), "version": version_str, "calls": current_calls})


@app.route("/gateway_status")
def gateway_status():
    return jsonify({"gateways": mqtt_parser.get_gateway_status()})


@app.route("/events")
def events():
    def event_stream():
        # Crea una coda per questo client
        q = queue.Queue(maxsize=100)
        with mqtt_parser.event_lock:
            mqtt_parser.event_subscribers.append(q)
            print(f"DEBUG SSE: Nuovo client connesso. Totale: {len(mqtt_parser.event_subscribers)}")

        try:
            # Opzionale: invia un evento 'connected'
            yield f"data: {json.dumps({'type': 'system', 'msg': 'connected'})}\n\n"
            
            while True:
                # Blocca finché non arriva un evento
                event = q.get()
                yield f"data: {json.dumps(event)}\n\n"
                q.task_done()
        except GeneratorExit:
            with mqtt_parser.event_lock:
                if q in mqtt_parser.event_subscribers:
                    mqtt_parser.event_subscribers.remove(q)
                    print(f"DEBUG SSE: Client disconnesso. Totale: {len(mqtt_parser.event_subscribers)}")
        except Exception as e:
            print(f"DEBUG SSE ERRORE: {e}")
            with mqtt_parser.event_lock:
                if q in mqtt_parser.event_subscribers:
                    mqtt_parser.event_subscribers.remove(q)

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")


@app.route("/repeaters")
def repeaters():
    return redirect(url_for('index'))


@app.route("/api/repeaters")
def get_repeaters_messages():
    with repeaters_messages_lock:
        return jsonify({'messages': repeaters_messages, 'last_update': repeaters_last_update})


@app.route("/api/export_calls")
def export_calls():
    import csv
    from io import StringIO
    import sqlite3
    
    si = StringIO()
    cw = csv.writer(si)
    # Header del CSV
    cw.writerow(['Data', 'Orario', 'Modo', 'Slot', 'Nominativo', 'Nome', 'Target (TG)', 'Nodo', 'Durata', 'BER', 'City', 'Country'])
    
    try:
        conn = sqlite3.connect(mqtt_parser.DB_PATH)
        c = conn.cursor()
        c.execute("SELECT data, orario, mode, slot, callsign, name, tg, nodo, duration, ber, city, country FROM calls ORDER BY id DESC")
        rows = c.fetchall()
        for row in rows:
            cw.writerow(row)
        conn.close()
    except Exception as e:
        print(f"Errore Export Calls: {e}")

    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=history_export.csv"
    output.headers["Content-type"] = "text/csv"
    return output


@app.route("/api/export_repeaters")
def export_repeaters():
    import csv
    from io import StringIO
    
    si = StringIO()
    cw = csv.writer(si)
    cw.writerow(['Node', 'Gateway', 'Topic', 'Callsign', 'ID', 'RX Frequency', 'TX Frequency', 'Power', 'Location', 'Last Seen'])
    
    with repeaters_messages_lock:
        for topic, payload in repeaters_messages.items():
            parts = topic.split('/')
            node = parts[1] if len(parts) > 1 else topic
            gateway = parts[2] if len(parts) > 2 else "General"
            
            # Gestione subtopic multipli
            subtopic = "/".join(parts[3:]) if len(parts) > 3 else "General"
            
            ts = repeaters_last_update.get(topic, 0)
            last_seen = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts)) if ts > 0 else "Mai"
            
            # Estrazione sicura dei principali campi tecnici
            cw.writerow([
                node, gateway, subtopic,
                payload.get('Callsign', ''),
                payload.get('Id', ''),
                payload.get('RXFrequency', ''),
                payload.get('TXFrequency', ''),
                payload.get('Power', ''),
                payload.get('Location', payload.get('Description', '')),
                last_seen
            ])
            
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=repeaters_status_export.csv"
    output.headers["Content-type"] = "text/csv"
    return output

@app.route("/clear", methods=["POST"])
def clear_history():
    import sqlite3

    conn = sqlite3.connect(mqtt_parser.DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM calls")
    conn.commit()
    conn.close()
    with mqtt_parser.calls_lock:
        mqtt_parser.calls.clear()
    return jsonify({"status": "ok"})


def watchdog_thread():
    import time
    # TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    # TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    while True:
        # Check telemetry ogni ora
        time.sleep(3600)
        current_time = time.time()
        # Check Node ogni 3 ore
        timeout_seconds = 3 * 3600
        
        with repeaters_messages_lock:
            for topic, last_ts in repeaters_last_update.items():
                if current_time - last_ts > timeout_seconds:
                    node_id = topic.split('/')[1] if len(topic.split('/')) > 1 else topic
                    msg = f"⚠️ ALARM: Il nodo {node_id} ({topic}) non ha aggiornato la telemetria da più di 3 ore."
                    print(f"[{time.strftime('%H:%M:%S')}] {msg}")
                    
                    # Telegram integration
                    # if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                    #     try:
                    #         import requests
                    #         requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                    #                       json={'chat_id': TELEGRAM_CHAT_ID, 'text': msg})
                    #     except Exception as e:
                    #         print(f"Telegram error: {e}")


def start_mqtt():
    mqtt_parser.start_mqtt()


if __name__ == "__main__":

    t = threading.Thread(target=start_mqtt)
    t.daemon = True
    t.start()

    rep_mqtt_thread = threading.Thread(target=repeaters_mqtt_loop, daemon=True)
    rep_mqtt_thread.start()

    rep_updater_thread = threading.Thread(target=repeaters_hourly_updater, daemon=True)
    rep_updater_thread.start()

    watchdog = threading.Thread(target=watchdog_thread, daemon=True)
    watchdog.start()


    try:
        from waitress import serve

        print(f"Avvio del server WSGI (Waitress) sulla porta {mqtt_parser.HTTP_PORT}...")
        serve(app, host="0.0.0.0", port=mqtt_parser.HTTP_PORT)
    except ImportError:
        print(
            f"Libreria Waitress non trovata. Esecuzione server di sviluppo Flask sulla porta {mqtt_parser.HTTP_PORT}..."
        )
        print(
            "Consiglio: per un ambiente stabile in produzione installa 'pip install waitress'."
        )
        app.run(host="0.0.0.0", port=mqtt_parser.HTTP_PORT)
