"""Test del parser MQTT e degli alert. Esecuzione: python -m unittest discover -s tests"""
import json
import os
import sys
import tempfile
import time
import types
import unittest

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "test.db")
os.environ.pop("KEEP_HISTORY", None)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import alerts  # noqa: E402
import mqtt_parser as mp  # noqa: E402


def msg(topic, payload):
    return types.SimpleNamespace(topic=topic, payload=json.dumps(payload).encode())


def db_calls():
    return mp.get_recent_calls(limit=100)


class ParserBase(unittest.TestCase):
    def setUp(self):
        mp.calls.clear()
        mp.gateway_status.clear()
        mp.tg_map.clear()
        with mp.sqlite3.connect(mp.DB_PATH) as c:
            c.execute("DELETE FROM calls")
            c.execute("DELETE FROM users")
            c.execute("DELETE FROM nxdn_users")
            c.execute("INSERT INTO users VALUES ('2222222','IU2ABC','Mario','Milano','Italy')")

    def start(self, **kw):
        d = {"action": "start", "slot": 2, "source_id": "2222222", "source_cs": "IU2ABC",
             "destination_id": 222, "source": "network"}
        d.update(kw)
        mp.on_message(None, None, msg("mmdvm/NODO1/json", {"DMR": d}))


class TestCallLifecycle(ParserBase):
    def test_start_resolves_user_and_tg(self):
        mp.tg_map["222"] = "Italia"
        self.start()
        c = db_calls()
        self.assertEqual(len(c), 1)
        self.assertEqual(c[0]["ID"], "IU2ABC")
        self.assertEqual(c[0]["NAME"], "Mario")
        self.assertEqual(c[0]["TG"], "222 (Italia)")
        self.assertEqual(c[0]["MODE"], "DMR")
        self.assertEqual(c[0]["TIME"], "")

    def test_unknown_user(self):
        self.start(source_id="9999999", source_cs="")
        self.assertEqual(db_calls()[0]["NAME"], "Unknown")

    def test_end_sets_duration_and_ber(self):
        self.start()
        mp.on_message(None, None, msg("mmdvm/NODO1/json",
                      {"DMR": {"action": "end", "slot": 2, "duration": 5.43, "ber": 0.04}}))
        c = db_calls()[0]
        self.assertEqual(c["TIME"], "5.4")
        self.assertEqual(c["BER"], "0.04%")
        self.assertEqual(c["is_idle"], 0)

    def test_lost_marks_duration(self):
        self.start()
        mp.on_message(None, None, msg("mmdvm/NODO1/json",
                      {"DMR": {"action": "lost", "slot": 2, "duration": 2}}))
        self.assertEqual(db_calls()[0]["TIME"], "2.0!")

    def test_duplicate_start_within_2s_ignored(self):
        self.start()
        self.start()
        self.assertEqual(len(db_calls()), 1)

    def test_new_start_closes_hanging_call(self):
        self.start()
        self.start(source_id="3333333", source_cs="IU2XYZ")
        c = db_calls()
        self.assertEqual(len(c), 2)
        old = [x for x in c if x["ID"] == "IU2ABC"][0]
        self.assertNotEqual(old["TIME"], "")

    def test_other_slot_not_closed(self):
        self.start(slot=1)
        self.start(slot=2)
        self.assertTrue(all(x["TIME"] == "" for x in db_calls()))

    def test_end_on_other_slot_does_not_close(self):
        self.start(slot=2)
        mp.on_message(None, None, msg("mmdvm/NODO1/json",
                      {"DMR": {"action": "end", "slot": 1, "duration": 3}}))
        self.assertEqual(db_calls()[0]["TIME"], "")

    def test_idle_closes_and_marks_listening(self):
        self.start()
        mp.on_message(None, None, msg("mmdvm/NODO1/json", {"MMDVM": {"mode": "idle"}}))
        c = db_calls()[0]
        self.assertNotEqual(c["TIME"], "")
        self.assertEqual(c["is_idle"], 1)

    def test_source_type_gateway(self):
        mp.on_message(None, None, msg("ysf-gateway/GW1/json",
                      {"YSF": {"action": "start", "source_cs": "IU2ABC", "reflector": "ITALY"}}))
        c = db_calls()[0]
        self.assertEqual(c["SOURCE_TYPE"], "GATEWAY")
        self.assertEqual(c["MODE"], "YSF")
        self.assertEqual(c["NAME"], "Mario")

    def test_text_updates_active_call(self):
        mp.on_message(None, None, msg("mmdvm/NODO1/json",
                      {"D-STAR": {"action": "start", "source_cs": "XX1XX"}}))
        mp.on_message(None, None, msg("mmdvm/NODO1/json",
                      {"TEXT": {"slot": "-", "value": "IZ1RFM Carlo"}}))
        c = db_calls()[0]
        self.assertEqual(c["ID"], "IZ1RFM")
        self.assertEqual(c["NAME"], "Carlo")

    def test_invalid_payload_does_not_crash(self):
        mp.on_message(None, None, types.SimpleNamespace(topic="mmdvm/N/json", payload=b"not json"))
        mp.on_message(None, None, msg("mmdvm/N/json", []))
        self.assertEqual(db_calls(), [])


class TestGatewayStatus(ParserBase):
    def test_link_status(self):
        mp.on_message(None, None, msg("dmr-gateway/GW1/json",
                      {"link": {"action": "linking", "talkgroup": 222}}))
        st = mp.get_gateway_status()
        self.assertEqual(len(st), 1)
        self.assertEqual(st[0]["type"], "DMR")
        self.assertEqual(st[0]["action"], "linking")
        self.assertEqual(st[0]["talkgroup"], "222")

    def test_status_message_is_online(self):
        mp.on_message(None, None, msg("p25-gateway/GW2/json", {"status": {"message": "ok"}}))
        self.assertEqual(mp.get_gateway_status()[0]["action"], "online")


class TestFormatBer(unittest.TestCase):
    def test_values(self):
        self.assertEqual(mp.format_ber(None), "0%")
        self.assertEqual(mp.format_ber(""), "0%")
        self.assertEqual(mp.format_ber("1.26%"), "1.3%")
        self.assertEqual(mp.format_ber(0.05), "0.05%")
        self.assertEqual(mp.format_ber("abc"), "abc")


class TestHistory(ParserBase):
    def test_purge_old_calls(self):
        self.start()
        with mp.sqlite3.connect(mp.DB_PATH) as c:
            c.execute("INSERT INTO calls (callsign, mode, slot, nodo, start_ts) VALUES ('OLD','DMR','1','N',?)",
                      (time.time() - 40 * 86400,))
        self.assertEqual(mp.purge_old_calls(30), 1)
        self.assertEqual(len(db_calls()), 1)

    def test_purge_disabled_with_zero(self):
        self.assertEqual(mp.purge_old_calls(0), 0)


class TestUserDbReload(ParserBase):
    def test_force_replaces_data(self):
        cwd = os.getcwd()
        d = tempfile.mkdtemp()
        os.chdir(d)
        try:
            with open("user.csv", "w") as f:
                f.write("RADIO_ID,CALLSIGN,FIRST_NAME,LAST_NAME,CITY,STATE,COUNTRY\n"
                        "1,AA1AA,Uno,,Roma,RM,Italy\n")
            mp.load_databases()  # tabella gia' popolata: non tocca nulla
            self.assertEqual(mp.get_user_info("2222222", "DMR")[0], "IU2ABC")
            mp.load_databases(force=True)
            self.assertEqual(mp.get_user_info("1", "DMR")[0], "AA1AA")
            self.assertEqual(mp.get_user_info("2222222", "DMR")[0], "2222222")
        finally:
            os.chdir(cwd)


class TestAlerts(unittest.TestCase):
    def test_offline_once_then_recovered(self):
        state = set()
        now = 100000
        last = {"dati/NODO1/a": now - 4 * 3600}
        off, rec = alerts.evaluate_nodes(last, now, 3 * 3600, state)
        self.assertEqual([n for n, _ in off], ["NODO1"])
        off, rec = alerts.evaluate_nodes(last, now + 600, 3 * 3600, state)
        self.assertEqual((off, rec), ([], []))  # nessun alert ripetuto
        last["dati/NODO1/a"] = now + 700
        off, rec = alerts.evaluate_nodes(last, now + 800, 3 * 3600, state)
        self.assertEqual(rec, ["NODO1"])

    def test_node_ok_if_any_topic_recent(self):
        last = {"dati/N/a": 0, "dati/N/b": 99999}
        off, _ = alerts.evaluate_nodes(last, 100000, 3600, set())
        self.assertEqual(off, [])

    def test_telegram_not_configured(self):
        os.environ.pop("TELEGRAM_BOT_TOKEN", None)
        self.assertFalse(alerts.send_telegram("x"))


if __name__ == "__main__":
    unittest.main()
