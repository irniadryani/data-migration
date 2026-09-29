import os
import sys
import json
import argparse
import psycopg2
from kafka import KafkaConsumer

from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.datastream.functions import ProcessAllWindowFunction

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from config import DB_CONFIG, KAFKA_BOOTSTRAP, KAFKA_HOST_BOOTSTRAP


def clean_column_name(name):
    if not name:
        return ""
    return str(name).strip().lower().replace(" ", "_").replace("-", "_")


def detect_data_type(column_name, value):
    lower_col = column_name.lower()
    if any(k in lower_col for k in ["price", "harga", "amount", "total", "saldo", "biaya"]) or isinstance(value, float):
        return "NUMERIC(12, 2)"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not any(k in lower_col for k in ["code", "id", "kode", "phone", "telepon", "zip"]):
        return "BIGINT"
    return "TEXT"


def get_kafka_topics(broker):
    try:
        consumer = KafkaConsumer(bootstrap_servers=broker, consumer_timeout_ms=3000)
        topics = [t for t in consumer.topics() if not t.startswith("_")]
        consumer.close()
        return topics
    except Exception as e:
        print(f"[ERROR] Failed to read Kafka topics: {e}")
        return []

class PostgresSink(ProcessAllWindowFunction):

    def __init__(self, db_config, table_name):
        self.db_config = db_config
        self.table_name = table_name.lower()
        self.conn = None
        self.cur = None
        self.existing_cols = set()
        self.primary_key = None

    def open(self, runtime_context):
        self.ensure_connection()
        self.load_table_schema()

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def ensure_connection(self):
        try:
            if not self.conn or self.conn.closed != 0:
                self.conn = psycopg2.connect(**self.db_config)
                self.conn.autocommit = False
                self.cur = self.conn.cursor()
                return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False
        return True

    def load_table_schema(self):
        #Read existing columns and primary key from PostgreSQL
        if not self.ensure_connection():
            return
        try:
            # Query existing column names
            self.cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = %s;
            """, (self.table_name,))
            self.existing_cols = {r[0].lower() for r in self.cur.fetchall()}

            # Query primary key constraint
            self.cur.execute("""
                SELECT kcu.column_name FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = 'public' AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY';
            """, (self.table_name,))
            pk = self.cur.fetchone()
            self.primary_key = pk[0].lower() if pk else None
            self.conn.commit()
        except Exception:
            if self.conn:
                self.conn.rollback()

    def create_table_if_missing(self, records):
        #create table if it does not exist
        all_cols = []
        sample_vals = {}
        for row in records:
            for k, v in row.items():
                clean_k = clean_column_name(k)
                if clean_k and clean_k not in all_cols:
                    all_cols.append(clean_k)
                    sample_vals[clean_k] = v

        if not all_cols:
            return

        pk = next((k for k in all_cols if k in ["id", f"{self.table_name}_id"]), all_cols[0])

        col_defs = []
        for col in all_cols:
            col_type = detect_data_type(col, sample_vals.get(col))
            if col == pk:
                col_defs.append(f"{col} VARCHAR(150) PRIMARY KEY")
            else:
                col_defs.append(f"{col} {col_type}")

        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} (\n    " + ",\n    ".join(col_defs) + "\n);"
        self.cur.execute(query)
        self.conn.commit()
        self.primary_key = pk
        self.existing_cols = set(all_cols)
        print(f"Created table '{self.table_name}' with primary key '{pk}'.")

    def add_new_columns_if_any(self, records):
        new_cols = {}
        for row in records:
            for k, v in row.items():
                clean_k = clean_column_name(k)
                if clean_k and clean_k not in self.existing_cols and clean_k not in new_cols:
                    new_cols[clean_k] = v

        for col, val in new_cols.items():
            col_type = detect_data_type(col, val)
            self.cur.execute(f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS {col} {col_type};")
            self.conn.commit()
            self.existing_cols.add(col)
            print(f"Added new column '{col}' ({col_type}) to table '{self.table_name}'.")

    def process(self, context, elements):
        raw_list = list(elements)
        if not raw_list or not self.ensure_connection():
            return

        # 1. Parse JSON records
        records = []
        for text in raw_list:
            if not text or not text.strip():
                continue
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    records.extend(parsed)
                elif isinstance(parsed, dict):
                    records.append(parsed)
            except Exception:
                continue

        if not records:
            return

        try:
            if not self.existing_cols:
                self.create_table_if_missing(records)

            self.add_new_columns_if_any(records)

            # 4. Insert / Upsert records
            saved_count = 0
            for row in records:
                clean_data = {clean_column_name(k): v for k, v in row.items() if clean_column_name(k) in self.existing_cols}
                if not clean_data:
                    continue

                cols = list(clean_data.keys())
                vals = [clean_data[c] for c in cols]
                cols_str = ", ".join(cols)
                placeholders = ", ".join(["%s"] * len(cols))

                if self.primary_key and self.primary_key in clean_data:
                    update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != self.primary_key])
                    if update_str:
                        sql = f"INSERT INTO {self.table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({self.primary_key}) DO UPDATE SET {update_str};"
                    else:
                        sql = f"INSERT INTO {self.table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({self.primary_key}) DO NOTHING;"
                else:
                    sql = f"INSERT INTO {self.table_name} ({cols_str}) VALUES ({placeholders});"

                self.cur.execute(sql, vals)
                saved_count += 1

            self.conn.commit()
            if saved_count > 0:
                yield f"Saved {saved_count} records into table '{self.table_name}'."
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            yield f" Failed to save records into '{self.table_name}': {e}"

def main():
    parser = argparse.ArgumentParser(description="PyFlink Kafka to PostgreSQL DataStream Pipeline")
    parser.add_argument("--topic", default="all", help="Target Kafka topic ('all' or specific topic name)")
    args, _ = parser.parse_known_args()

    is_docker = os.path.exists("/.dockerenv") or os.path.exists("/opt/flink")
    broker = KAFKA_BOOTSTRAP if is_docker else KAFKA_HOST_BOOTSTRAP

    db = dict(DB_CONFIG)
    if not is_docker and db.get("host") == "postgres_new_db":
        db["host"] = "localhost"
        db["port"] = 5433

    target_topics = [args.topic.strip()] if args.topic and args.topic.lower() not in ("all", "auto", "*") else get_kafka_topics(broker)
    if not target_topics:
        print("No active Kafka topics found.")
        return

    print(f"Starting streaming pipeline for topics: {target_topics}")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(5000)

    jar_dir = "/opt/flink/usrlib" if is_docker else os.path.abspath(os.path.join(CURRENT_DIR, "..", "usrlib"))
    if os.path.exists(jar_dir):
        jars = [f"file://{os.path.join(jar_dir, f)}" for f in os.listdir(jar_dir) if f.endswith(".jar")]
        if jars:
            env.add_jars(*jars)

    # Build stream for each topic
    for topic in target_topics:
        table = topic.lower()
        source = KafkaSource.builder() \
            .set_bootstrap_servers(broker) \
            .set_topics(topic) \
            .set_group_id(f"flink_pg_{table}_group") \
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        stream = env.from_source(source, WatermarkStrategy.no_watermarks(), f"Kafka_{topic}")
        stream.window_all(TumblingProcessingTimeWindows.of(Time.seconds(1))) \
              .process(PostgresSink(db, table)) \
              .print()

    # Dynamic informative job name
    if len(target_topics) == 1:
        job_name = f"Insert into table postgres_{target_topics[0]}"
    else:
        job_name = f"Insert into tables postgres_[{', '.join(target_topics)}]"

    print(f"[INFO] Running Flink job: '{job_name}'")
    env.execute(job_name)


if __name__ == '__main__':
    main()