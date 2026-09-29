import json
import csv
import os
import sys
import ssl
import xmlrpc.client
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer

JOBS_DIR = os.path.dirname(os.path.abspath(__file__))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

try:
    from config import (
        ODOO_URL,
        ODOO_DB,
        ODOO_USER,
        ODOO_PASSWORD,
        ODOO_DISPATCHER_GROUP_ID,
        KAFKA_HOST_BOOTSTRAP,
    )
except ImportError as err:
    print(f"\nGagal memuat file konfigurasi 'jobs/config.py': {err}")
    sys.exit(1)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


class RejectionLogger:
    """Mencatat record yang gagal diimpor ke file .log dan .csv."""

    def __init__(self, log_dir=None):
        self.log_dir = os.path.abspath(log_dir or os.path.join(os.path.dirname(__file__), "..", "logs"))
        os.makedirs(self.log_dir, exist_ok=True)

        self.error_txt_path = os.path.join(self.log_dir, "import_errors.log")
        self.failed_csv_path = os.path.join(self.log_dir, "failed_records.csv")

        if not os.path.exists(self.failed_csv_path) or os.path.getsize(self.failed_csv_path) == 0:
            with open(self.failed_csv_path, "w", encoding="utf-8", newline="") as f:
                csv.writer(f, delimiter=";").writerow(
                    ["timestamp", "topic_name", "odoo_model", "document_ref", "error_reason", "raw_payload_json"]
                )

    @staticmethod
    def _guess_ref(record):
        if not isinstance(record, dict):
            return "UNKNOWN"
        for key in ("note", "origin", "name", "code", "default_code"):
            if record.get(key):
                return record[key]
        return "UNKNOWN"

    def log_failure(self, topic, model, record, reason, doc_ref=None):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ref = doc_ref or self._guess_ref(record)
        payload_str = json.dumps(record, ensure_ascii=False) if isinstance(record, (dict, list)) else str(record)

        with open(self.failed_csv_path, "a", encoding="utf-8", newline="") as f:
            csv.writer(f, delimiter=";").writerow([ts, topic, model, ref, reason, payload_str])

        with open(self.error_txt_path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}]Topik: {topic} | Model: {model} | Ref: {ref}\n")
            f.write(f"    Alasan : {reason}\n")
            f.write(f"    Payload: {payload_str}\n")
            f.write("-" * 80 + "\n")

        print(f"     REJECTED ({model}) {ref} -> {reason}")

class UniversalOdooEngine:
    def __init__(self, odoo_url, odoo_db, odoo_user, odoo_password, require_lot=False):
        self.url = (odoo_url or "").rstrip("/")
        self.db = odoo_db
        self.user = odoo_user
        self.password = odoo_password
        self.require_lot = require_lot

        self.logger = RejectionLogger()
        self.fields_cache = {}
        self.verified_models = set()

        proxy_kwargs = {"allow_none": True}
        if self.url.lower().startswith("https://"):
            try:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                proxy_kwargs["context"] = ctx
            except Exception:
                pass

        self.common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common", **proxy_kwargs)
        self.models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object", **proxy_kwargs)

        try:
            self.uid = self.common.authenticate(self.db, self.user, self.password, {})
        except Exception as conn_err:
            print(f"\n ERROR: Gagal terhubung ke server Odoo di '{self.url}': {conn_err}")
            sys.exit(1)

        if not self.uid:
            print("\n ERROR: Login ke Odoo gagal!")
            sys.exit(1)

        print(f"Terhubung ke Odoo ({self.url} | DB: {self.db} | UID: {self.uid})")

    def transform_model(self, topic_name):
        normalized = topic_name.lower().strip()
        if self.is_model_exist_in_odoo(normalized):
            return normalized

        candidate = normalized.replace("_", ".").replace("-", ".")
        if self.is_model_exist_in_odoo(candidate):
            return candidate

        return None

    def is_model_exist_in_odoo(self, model_name):
        if model_name in self.verified_models:
            return True
        try:
            ids = self.models.execute_kw(
                self.db, self.uid, self.password, "ir.model", "search", [[("model", "=", model_name)]]
            )
        except Exception:
            return False
        if ids:
            self.verified_models.add(model_name)
            return True
        return False

    def get_valid_fields(self, model_name):
        if model_name not in self.fields_cache:
            self.fields_cache[model_name] = self.models.execute_kw(
                self.db, self.uid, self.password, model_name, "fields_get", [], {"attributes": ["type"]}
            )
        return self.fields_cache[model_name]

    def _validation_error(self, rec):
        if self.require_lot:
            lot_keys = [k for k in rec if "lot" in k.lower()]
            if lot_keys and not any(str(rec.get(k, "")).strip() for k in lot_keys):
                return "Nomor Lot/Batch kosong (Wajib diisi)"

        for k in rec:
            if "quantity" not in k.lower() and "qty" not in k.lower():
                continue
            val = rec.get(k)
            try:
                if float(str(val).replace(",", ".").strip()) <= 0:
                    return f"Quantity '{k}' harus > 0 (Nilai: '{val}')"
            except (ValueError, TypeError):
                return f"Quantity '{k}' tidak valid (Nilai: '{val}')"

        return None

    # ---------- pembentukan header/rows untuk load() ----------

    @staticmethod
    def _build_clean_headers(records, valid_fields):
        raw_headers = list(dict.fromkeys(k for r in records for k in r.keys()))
        has_id_col = any(h.endswith("/.id") for h in raw_headers)

        headers = []
        for h in raw_headers:
            root = h.split("/")[0].strip()
            if root not in valid_fields:
                continue
            if has_id_col and f"{h}/.id" in raw_headers:
                continue
            if "date" in h and any(isinstance(r.get(h), str) and "/" in r.get(h) for r in records):
                continue
            headers.append(h)
        return headers

    @staticmethod
    def _build_rows(records, headers):
        return [["" if r.get(col) is None else str(r.get(col)) for col in headers] for r in records]

    #kirim data ke fungsi load() odoo
    def _load_batch(self, model, headers, rows, label="SUKSES"):
        try:
            res = self.models.execute_kw(self.db, self.uid, self.password, model, "load", [headers, rows])
        except Exception as exc:
            print(f" Error RPC Odoo ({model}): {exc}")
            return None
        ids = res.get("ids") or []
        if ids:
            print(f"{label}: {len(ids)} record dibuat di '{model}' (IDs: {ids})")
        return res

    def process_topic_batch(self, topic_name, raw_records):
        target_model = self.resolve_model(topic_name)
        if not target_model:
            print(f" Topik '{topic_name}' tidak dapat dipetakan ke model Odoo. Dilewati.")
            for r in raw_records:
                self.logger.log_failure(topic_name, "UNKNOWN", r, f"Topic '{topic_name}' unmapped to Odoo model")
            return

        flat_records = []
        for item in raw_records:
            if isinstance(item, list):
                flat_records.extend(item)
            elif isinstance(item, dict):
                flat_records.append(item)
        if not flat_records:
            return

        valid_records = []
        for rec in flat_records:
            if not isinstance(rec, dict):
                continue
            error = self._validation_error(rec)
            if error:
                self.logger.log_failure(topic_name, target_model, rec, error)
            else:
                valid_records.append(rec)
        if not valid_records:
            return

        valid_fields = self.get_valid_fields(target_model)
        headers = self._build_clean_headers(valid_records, valid_fields)
        rows = self._build_rows(valid_records, headers)

        print(f"[{topic_name} -> {target_model}] Mengirim {len(rows)} baris ke Odoo load()...")
        res = self._load_batch(target_model, headers, rows)
        if res is None:
            for r in valid_records:
                self.logger.log_failure(topic_name, target_model, r, "RPC Error saat load()")
            return

        if not res.get("ids"):
            print(f"Tidak ada record yang dibuat di '{target_model}'.")

        error_rows = set()
        messages = res.get("messages") or []
        if messages:
            print(f" {len(messages)} pesan error validasi Odoo:")
            for m in messages:
                row_idx = m.get("rows", {}).get("from", 0)
                field_name = m.get("field_name") or m.get("field") or "General"
                if m.get("type") == "error":
                    error_rows.add(row_idx)
                failed_rec = valid_records[row_idx] if row_idx < len(valid_records) else {}
                self.logger.log_failure(topic_name, target_model, failed_rec, f"Field '{field_name}': {m.get('message', 'Validation error')}")

        # Upaya salvage baris yang tidak error jika batch sebagian gagal
        if not res.get("ids") and error_rows and len(error_rows) < len(valid_records):
            salvaged = [r for i, r in enumerate(valid_records) if i not in error_rows]
            if salvaged:
                print(f"Menyelamatkan {len(salvaged)} record valid dari batch...")
                self._load_batch(target_model, headers, self._build_rows(salvaged, headers), label="SUKSES (SALVAGE)")

def run_auto_dispatcher(bootstrap_servers, topic_arg, group_id, odoo_url, odoo_db, odoo_user, odoo_password, require_lot=False, once=False):
    print("Dispatcher Kafka -> Odoo aktif")
    print(f"    Broker Kafka : {bootstrap_servers}")
    print(f"    Target Topik : {topic_arg}")
    print(f"    Target Odoo  : {odoo_url} (DB: {odoo_db})")

    engine = UniversalOdooEngine(odoo_url, odoo_db, odoo_user, odoo_password, require_lot=require_lot)

    consumer_kwargs = {
        "bootstrap_servers": bootstrap_servers.split(","),
        "auto_offset_reset": "earliest",
        "enable_auto_commit": True,
        "group_id": group_id,
        "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
    }

    try:
        if topic_arg in ("all", "auto", "*"):
            consumer = KafkaConsumer(**consumer_kwargs)
            consumer.subscribe(pattern=r"^(?!_schemas|__consumer).*$")
        else:
            topics_list = [t.strip() for t in topic_arg.split(",") if t.strip()]
            consumer = KafkaConsumer(*topics_list, **consumer_kwargs)
    except Exception as k_err:
        print(f"\n ERROR: Gagal terhubung ke Kafka broker di '{bootstrap_servers}': {k_err}")
        sys.exit(1)

    print("Terhubung ke Kafka. Menunggu pesan...\n")

    batch_by_topic = defaultdict(list)
    idle_poll_count = 0

    def flush_batches():
        for topic_name, records in list(batch_by_topic.items()):
            try:
                engine.process_topic_batch(topic_name, records)
            except Exception as proc_err:
                print(f" Gagal memproses topik '{topic_name}': {proc_err}")
        batch_by_topic.clear()

    try:
        while True:
            raw_batch = consumer.poll(timeout_ms=2000, max_records=500)

            if raw_batch:
                for tp, messages in raw_batch.items():
                    batch_by_topic[tp.topic].extend(msg.value for msg in messages)
                total_msgs = sum(len(v) for v in batch_by_topic.values())
                print(f"Menerima {total_msgs} record dari {len(batch_by_topic)} topik Kafka...")
                idle_poll_count = 0
                continue

            if batch_by_topic:
                flush_batches()
                if once:
                    print("Mode --once selesai.")
                    break
            else:
                idle_poll_count += 1
                if once and idle_poll_count >= 2:
                    print("Tidak ada pesan baru di Kafka. Selesai.")
                    break

    except KeyboardInterrupt:
        print("\nConsumer dihentikan oleh user.")
        flush_batches()
    finally:
        consumer.close()
        print("Kafka Consumer ditutup.")

if __name__ == "__main__":
    is_once = "--once" in sys.argv

    run_auto_dispatcher(
        bootstrap_servers=KAFKA_HOST_BOOTSTRAP,
        topic_arg="all",
        group_id=ODOO_DISPATCHER_GROUP_ID,
        odoo_url=ODOO_URL,
        odoo_db=ODOO_DB,
        odoo_user=ODOO_USER,
        odoo_password=ODOO_PASSWORD,
        once=is_once,
    )