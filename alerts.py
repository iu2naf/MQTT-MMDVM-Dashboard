"""Alert Telegram per i nodi che smettono di inviare telemetria."""
import json
import os
import urllib.request


def evaluate_nodes(last_updates, now, timeout, state):
    """Confronta gli ultimi aggiornamenti con il timeout.

    last_updates: {topic: timestamp}; state: set dei nodi gia' segnalati
    (modificato in place). Restituisce (offline, recovered): liste di nodi
    appena risultati offline / tornati online. Ogni cambio di stato viene
    segnalato una sola volta.
    """
    # Un nodo e' ok se almeno un suo topic e' recente.
    newest = {}
    for topic, ts in last_updates.items():
        parts = topic.split("/")
        node = parts[1] if len(parts) > 1 else topic
        newest[node] = max(newest.get(node, 0), ts)

    offline, recovered = [], []
    for node, ts in sorted(newest.items()):
        stale = (now - ts) > timeout
        if stale and node not in state:
            state.add(node)
            offline.append((node, now - ts))
        elif not stale and node in state:
            state.discard(node)
            recovered.append(node)
    return offline, recovered


def send_telegram(text):
    """Invia un messaggio Telegram. Ritorna True se inviato, False se
    non configurato o in errore. Token e chat id arrivano dal .env."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip().strip('"')
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip().strip('"')
    if not token or not chat_id or token == "TELEGRAM_BOT_TOKEN":
        return False
    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=json.dumps({"chat_id": chat_id, "text": text}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
        return True
    except Exception as e:
        print(f"Errore invio Telegram: {type(e).__name__}")
        return False
