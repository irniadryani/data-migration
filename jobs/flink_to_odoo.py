import os
import sys
import json
import argparse
import xmlrpc.client

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
)

class OdooBatchWindow(ProcessAllWindowFunction):

    def __init__(self, odoo_url, odoo_db, odoo_user, odoo_password, default_topic):
        self.url = (odoo_url or "").rstrip("/")
        self.db = odoo_db
        self.user = odoo_user
        self.password = odoo_password
        self.default_topic = default_topic
        self.uid = None
        self.models = None
        self.fields_cache = {}

    def _get_valid_fields(self, model):
        if model not in self.fields_cache:
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

    def open(self, runtime_context):
        self._ensure_connection()

    def _ensure_connection(self):
        if self.uid:
            return True
        try:
            proxy_kwargs = {"allow_none": True}
            common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common", **proxy_kwargs)
            self.models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object", **proxy_kwargs)
            self.uid = common.authenticate(self.db, self.user, self.password, {})
            if self.uid:
                print(f"Terhubung ke Odoo: {self.url} | DB: {self.db} | UID: {self.uid}")
                return True
        except Exception as exc:
            print(f"Menunggu server Odoo ({self.url}): {exc}")
        return False

    def process(self, context, elements):
        raw_list = list(elements)
        if not raw_list:
            return

        if not self._ensure_connection():
            yield f"Gagal terhubung ke Odoo di '{self.url}'"
            return

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
        target_model = topic.lower().replace("_", ".").replace("-", ".")

        valid_fields = self._get_valid_fields(target_model)
        raw_headers = [k for k in dict.fromkeys(k for r in records for k in r.keys()) if not k.startswith("_")]
        headers = [h for h in raw_headers if not valid_fields or h.split("/")[0] in valid_fields]
        if not headers:
            yield f"[{target_model}] Tidak ada field yang cocok dengan schema Odoo"
            return

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

        try:
            res = self.models.execute_kw(
                self.db, self.uid, self.password,
                target_model, "load",
                [headers, rows]
            )

            created_ids = res.get("ids") or []
            messages = res.get("messages") or []

            result_str = f" [{target_model}] Berhasil input {len(created_ids)} record dari {len(rows)} baris (IDs: {created_ids})"
            if messages:
                errors = [m.get("message") for m in messages if m.get("type") == "error"]
                if errors:
                    result_str += f" | Warning: {'; '.join(errors[:2])}"

            yield result_str

        except Exception as rpc_err:
            yield f"[{target_model}] RPC Error Odoo: {rpc_err}"

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
        print(f"[WARN] [Kafka Discovery] Gagal inspeksi metadata Kafka: {exc}")

    return topics_info


def main():
    parser = argparse.ArgumentParser(description="Flink Streaming Pipeline: Kafka to Odoo")
    parser.add_argument(
        "--topic",
        default=None,
        help="Nama topik Kafka (opsional). Jika tidak diisi, Flink otomatis membaca semua topik dan data unread."
    )
    parser.add_argument("--group", default="flink_odoo_group", help="Kafka consumer group ID")
    args, _ = parser.parse_known_args()

    is_docker = os.path.exists("/.dockerenv")
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
            print("Tidak ditemukan topik di Kafka")
            return

        target_topics = list(topics_lag.keys())
        for t, unread in topics_lag.items():
            model = t.replace("_", ".")

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
            .process(OdooBatchWindow(odoo_url, ODOO_DB, ODOO_USER, ODOO_PASSWORD, default_topic=topic))

        result_stream.print()

    print(f"\n Menjalankan pipeline streaming Flink untuk {len(target_topics)} topik...")
    env.execute("flink_to_odoo_unified")


if __name__ == "__main__":
    main()
