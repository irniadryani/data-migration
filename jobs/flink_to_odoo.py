import os
import sys
import json
import csv
import ssl
import argparse
import xmlrpc.client
from datetime import datetime

from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.datastream.functions import ProcessAllWindowFunction

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from config import (
    KAFKA_BOOTSTRAP,
    KAFKA_HOST_BOOTSTRAP,
    ODOO_URL,
    ODOO_DB,
    ODOO_USER,
    ODOO_PASSWORD,
    ODOO_DISPATCHER_GROUP_ID,
)


class RejectionLogger:
    def __init__(self, log_dir=None):
        if not log_dir:
            if os.path.exists("/opt/flink/project_logs"):
                log_dir = "/opt/flink/project_logs"
            else:
                log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))
        self.log_dir = os.path.abspath(log_dir)
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
                return str(record[key])
        return "UNKNOWN"

    def log_failure(self, topic, model, record, reason, doc_ref=None):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ref = doc_ref or self._guess_ref(record)
        payload_str = json.dumps(record, ensure_ascii=False) if isinstance(record, (dict, list)) else str(record)

        try:
            with open(self.failed_csv_path, "a", encoding="utf-8", newline="") as f:
                csv.writer(f, delimiter=";").writerow([ts, topic, model, ref, reason, payload_str])

            with open(self.error_txt_path, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] Topik: {topic} | Model: {model} | Ref: {ref}\n")
        except Exception as err:
            print(f"Gagal menulis log rejection: {err}")


class OdooBatchWindow(ProcessAllWindowFunction):
    def __init__(self, odoo_url, odoo_db, odoo_user, odoo_password, default_topic, require_lot=False):
        self.url = (odoo_url or "").rstrip("/")
        self.db = odoo_db
        self.user = odoo_user
        self.password = odoo_password
        self.default_topic = default_topic
        self.require_lot = require_lot

        self.uid = None
        self.models = None
        self.fields_cache = {}
        self.verified_models = set()
        self.logger = None

    def open(self, runtime_context):
        self.logger = RejectionLogger()
        self._ensure_connection()

    def _ensure_connection(self):
        if self.uid and self.models:
            return True
        try:
            proxy_kwargs = {"allow_none": True}
            if self.url.lower().startswith("https://"):
                try:
                    ctx = ssl.create_default_context()
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                    proxy_kwargs["context"] = ctx
                except Exception:
                    pass

            common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common", **proxy_kwargs)
            self.models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object", **proxy_kwargs)
            self.uid = common.authenticate(self.db, self.user, self.password, {})
            if self.uid:
                return True
        except Exception as exc:
            print(f"Menunggu koneksi Odoo ({self.url}): {exc}")
        return False

    def is_model_exist(self, model_name):
        if model_name in self.verified_models:
            return True
        if not self._ensure_connection():
            return False
        try:
            ids = self.models.execute_kw(
                self.db, self.uid, self.password, "ir.model", "search", [[("model", "=", model_name)]]
            )
            if ids:
                self.verified_models.add(model_name)
                return True
        except Exception:
            return False
        return False

    def resolve_model(self, topic_name):
        normalized = topic_name.lower().strip()
        if self.is_model_exist(normalized):
            return normalized
        candidate = normalized.replace("_", ".").replace("-", ".")
        if self.is_model_exist(candidate):
            return candidate
        return None

    def _get_valid_fields(self, model):
        if model not in self.fields_cache:
            if not self._ensure_connection():
                return {}
            try:
                res = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    model, "fields_get", [],
                    {"attributes": ["type"]}
                )
                self.fields_cache[model] = res or {}
            except Exception:
                self.fields_cache[model] = {}
        return self.fields_cache[model]

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

    @staticmethod
    def _build_clean_headers(records, valid_fields):
        raw_headers = list(dict.fromkeys(k for r in records for k in r.keys() if not k.startswith("_")))
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
        rows = []
        for r in records:
            row = []
            for col in headers:
                val = r.get(col)
                if val is None or str(val).strip() in ("", "None", "null"):
                    row.append("")
                else:
                    row.append(str(val))
            rows.append(row)
        return rows

    def _load_batch(self, model, headers, rows):
        try:
            return self.models.execute_kw(
                self.db, self.uid, self.password,
                model, "load",
                [headers, rows]
            )
        except Exception as exc:
            return {"error": str(exc)}

    def process(self, context, elements):
        raw_list = list(elements)
        if not raw_list:
            return

        if not self._ensure_connection():
            yield f" Gagal terhubung ke Odoo di '{self.url}'"
            return

        if self.logger is None:
            self.logger = RejectionLogger()

        records = []
        for raw_json_str in raw_list:
            if not raw_json_str or not raw_json_str.strip():
                continue
            try:
                item = json.loads(raw_json_str)
                if isinstance(item, list):
                    records.extend(item)
                elif isinstance(item, dict):
                    records.append(item)
            except Exception:
                continue

        if not records:
            return

        topic = records[0].get("_topic") or self.default_topic
        target_model = self.resolve_model(topic)
        if not target_model:
            yield f"Topik '{topic}' tidak cocok dengan model Odoo aktif. Dilewati."
            for r in records:
                self.logger.log_failure(topic, "UNKNOWN", r, f"Topic '{topic}' unmapped to Odoo model")
            return

        valid_records = []
        for rec in records:
            if not isinstance(rec, dict):
                continue
            err = self._validation_error(rec)
            if err:
                self.logger.log_failure(topic, target_model, rec, err)
            else:
                valid_records.append(rec)

        if not valid_records:
            yield f" [{target_model}] Semua record ({len(records)}) gagal validasi lokal. Dicatat ke logs/."
            return

        valid_fields = self._get_valid_fields(target_model)
        headers = self._build_clean_headers(valid_records, valid_fields)
        if not headers:
            yield f" [{target_model}] Tidak ada field yang cocok dengan schema Odoo"
            return

        rows = self._build_rows(valid_records, headers)
        res = self._load_batch(target_model, headers, rows)

        if isinstance(res, dict) and "error" in res:
            yield f" [{target_model}] RPC Error Odoo: {res['error']}"
            for r in valid_records:
                self.logger.log_failure(topic, target_model, r, f"RPC Error: {res['error']}")
            return

        created_ids = res.get("ids") or []
        messages = res.get("messages") or []

        if created_ids:
            yield f" [{target_model}] SUKSES input {len(created_ids)} record dari {len(rows)} baris (IDs: {created_ids})"
            return
        
        error_rows = set()
        if messages:
            for m in messages:
                row_idx = m.get("rows", {}).get("from", 0)
                field_name = m.get("field_name") or m.get("field") or "General"
                if m.get("type") == "error":
                    error_rows.add(row_idx)
                failed_rec = valid_records[row_idx] if row_idx < len(valid_records) else {}
                self.logger.log_failure(
                    topic, target_model, failed_rec,
                    f"Field '{field_name}': {m.get('message', 'Validation error')}"
                )

        if error_rows and len(error_rows) < len(valid_records):
            salvaged = [r for i, r in enumerate(valid_records) if i not in error_rows]
            if salvaged:
                salvaged_rows = self._build_rows(salvaged, headers)
                res_salvage = self._load_batch(target_model, headers, salvaged_rows)
                salvaged_ids = (res_salvage.get("ids") or []) if isinstance(res_salvage, dict) else []
                if salvaged_ids:
                    yield f" [{target_model}] SUKSES (SALVAGE) input {len(salvaged_ids)} record valid dari {len(salvaged_rows)} baris (IDs: {salvaged_ids})"
                    return

        yield f" [{target_model}] Gagal memproses {len(valid_records)} record. {len(messages)} error dicatat ke logs/."


def get_kafka_topics_and_lag(broker, group_id):
    from kafka import KafkaConsumer, TopicPartition

    topics_info = {}
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=broker,
            group_id=group_id,
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
        )
        raw_topics = [t for t in consumer.topics() if not t.startswith("_") and not t.startswith("__")]

        for t in sorted(raw_topics):
            partitions = consumer.partitions_for_topic(t) or []
            tps = [TopicPartition(t, p) for p in partitions]
            if not tps:
                topics_info[t] = 0
                continue

            end_offsets = consumer.end_offsets(tps)
            total_unread = 0
            for tp in tps:
                committed = consumer.committed(tp) or 0
                end = end_offsets.get(tp, 0)
                total_unread += max(0, end - committed)
            topics_info[t] = total_unread

        consumer.close()
    except Exception as exc:
        print(f"Gagal inspeksi metadata Kafka: {exc}")

    return topics_info


def main():
    parser = argparse.ArgumentParser(description="Flink Streaming Pipeline: Kafka to Odoo")
    parser.add_argument(
        "--topic",
        default=None,
        help="Nama topik Kafka (opsional). Jika tidak diisi, Flink otomatis membaca semua topik dan data unread."
    )
    parser.add_argument("--group", default=ODOO_DISPATCHER_GROUP_ID, help="Kafka consumer group ID")
    parser.add_argument("--require-lot", action="store_true", help="Wajibkan nomor lot/batch terisi")
    args, _ = parser.parse_known_args()

    is_docker = os.path.exists("/.dockerenv") or os.path.exists("/opt/flink")
    kafka_broker = KAFKA_BOOTSTRAP if is_docker else KAFKA_HOST_BOOTSTRAP
    group_id = args.group

    odoo_url = ODOO_URL
    if is_docker and "localhost" in odoo_url:
        odoo_url = odoo_url.replace("localhost", "host.docker.internal")

    if args.topic and args.topic.lower() not in ("all", "auto", "*"):
        target_topics = [args.topic.strip()]
        print(f" Mode Single Topic: '{args.topic}'")
    else:
        topics_lag = get_kafka_topics_and_lag(kafka_broker, group_id)
        if not topics_lag:
            print(" Tidak ditemukan topik di Kafka")
            return

        target_topics = list(topics_lag.keys())
        print(f" Ditemukan {len(target_topics)} topik aktif di Kafka: {target_topics}")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(5000)

    jar_dir = "/opt/flink/usrlib" if is_docker else os.path.abspath(os.path.join(CURRENT_DIR, "..", "usrlib"))
    if os.path.exists(jar_dir):
        jars = [f"file://{os.path.join(jar_dir, f)}" for f in os.listdir(jar_dir) if f.endswith(".jar")]
        if jars:
            env.add_jars(*jars)

    for topic in target_topics:
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_broker) \
            .set_topics(topic) \
            .set_group_id(group_id) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        stream = env.from_source(
            source=kafka_source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name=f"KafkaSource_{topic}"
        )

        result_stream = stream \
            .window_all(TumblingProcessingTimeWindows.of(Time.seconds(2))) \
            .process(OdooBatchWindow(
                odoo_url,
                ODOO_DB,
                ODOO_USER,
                ODOO_PASSWORD,
                default_topic=topic,
                require_lot=args.require_lot
            ))

        result_stream.print()

    print(f"\n Menjalankan pipeline streaming Flink untuk {len(target_topics)} topik...")
    env.execute("flink_to_odoo_unified")


if __name__ == "__main__":
    main()

