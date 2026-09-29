import os

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres_new_db"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "migrate_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres")
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:29092")
KAFKA_HOST_BOOTSTRAP = os.getenv("KAFKA_HOST_BOOTSTRAP", "localhost:9092")

FLINK_API = os.getenv("FLINK_API", "http://localhost:8081")

CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", 10))

# local
ODOO_URL = os.getenv("ODOO_URL", "http://localhost:8069")
ODOO_DB = os.getenv("ODOO_DB", "test_db_migrate_2")
ODOO_USER = os.getenv("ODOO_USER", "odoo")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "odoo")
ODOO_DISPATCHER_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "odoo_auto_dispatcher_v2")

#staging
# ODOO_URL = os.getenv("ODOO_URL", "https://odoo-css.stagingapps.net")
# ODOO_DB = os.getenv("ODOO_DB", "cssdb_10")
# ODOO_USER = os.getenv("ODOO_USER", "admin")
# ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "Admin!@#")
# ODOO_DISPATCHER_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "group_css_4")

