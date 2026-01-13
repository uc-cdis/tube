import os
import tube.enums as enums

from cdislogging import get_logger
from tube.config_helper import find_paths, load_json
from .utils.general import get_resource_paths_from_yaml

logger = get_logger("__name__", log_level="warn")

LIST_TABLES_FILES = "tables.txt"

#
# Load db credentials from a creds.json file.
# See config_helper.py for paths searched for creds.json
# ex: export XDG_DATA_HOME="$HOME/.local/share"
#    and setup $XDG_DATA_HOME/.local/share/gen3/tube/creds.json
#
conf_data = load_json("creds.json", "tube")
DB_HOST = os.getenv("DB_HOST") or conf_data.get("db_host", "localhost")
DB_PORT = os.getenv("DB_PORT") or conf_data.get("db_port", "5432")
DB_DATABASE = os.getenv("DB_DATABASE") or conf_data.get("db_database", "sheepdog")
DB_USERNAME = os.getenv("DB_USERNAME") or conf_data.get("db_username", "peregrine")
DB_PASSWORD = os.getenv("DB_PASSWORD") or conf_data.get("db_password", "unknown")
ENV_DB_USE_SSL_BOOL = os.getenv("DB_USE_SSL", "false").lower() in ("true", "1", "t")

DB_USE_SSL = ENV_DB_USE_SSL_BOOL or conf_data.get(
    "db_use_ssl", False
)  # optional property to db_use_ssl
JDBC = "jdbc:postgresql://{}:{}/{}".format(DB_HOST, DB_PORT, DB_DATABASE)
PYDBC = "postgresql://{}:{}@{}:{}/{}".format(
    DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE
)
if DB_USE_SSL:
    JDBC += "?sslmode=require"
    PYDBC += "?sslmode=require"
DICTIONARY_URL = os.getenv(
    "DICTIONARY_URL",
    "https://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json",
)
ES_URL = os.getenv("ES_URL", "esproxy-service")
ES_PORT = os.getenv("ES_PORT", "9200")
ENV_ES_USE_SSL_BOOL = os.getenv("ES_USE_SSL", "false").lower() in ("true", "1", "t")
ES_USE_SSL = ENV_ES_USE_SSL_BOOL or int(ES_PORT) == 443
ES_AUTH_USERNAME = os.getenv("ES_AUTH_USERNAME")
ES_AUTH_PASSWORD = os.getenv("ES_AUTH_PASSWORD")

HDFS_DIR = "/result"
# Three modes: Test, Dev, Prod
RUNNING_MODE = os.getenv("RUNNING_MODE", enums.RUNNING_MODE_DEV)  # 'Prod' or 'Dev'

PARALLEL_JOBS = 1
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

ES_SPARK_CONFIG = {
    "es.nodes": ES_URL,
    "es.port": ES_PORT,
    "es.net.ssl": ES_USE_SSL,
    "es.input.json": "yes",
    "es.nodes.client.only": "false",
    "es.nodes.discovery": "false",
    "es.nodes.data.only": "false",
    "es.nodes.wan.only": "true",
}

ES_CONNECTION_CONFIG = {
    "host": ES_URL,
    "port": int(ES_PORT),
    "scheme": "https" if ES_USE_SSL else "http",
}
es_is_basic_auth_used = bool(ES_AUTH_USERNAME) and bool(ES_AUTH_PASSWORD)
if es_is_basic_auth_used:
    ES_CONNECTION_CONFIG["http_auth"] = (ES_AUTH_USERNAME, ES_AUTH_PASSWORD)
    ES_SPARK_CONFIG["es.net.http.auth.user"] = ES_AUTH_USERNAME
    ES_SPARK_CONFIG["es.net.http.auth.pass"] = ES_AUTH_PASSWORD

HADOOP_HOME = os.getenv("HADOOP_HOME", "/usr/local/Cellar/hadoop/3.1.0/libexec/")
JAVA_HOME = os.getenv(
    "JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home"
)
HADOOP_URL = os.getenv("HADOOP_URL", "http://spark-service:9000")
ES_HADOOP_VERSION = os.getenv("ES_HADOOP_VERSION", "")
OPENSEARCH_HADOOP_VERSION = os.getenv("OPENSEARCH_HADOOP_VERSION", "1.0.1")
ES_HADOOP_HOME_BIN = "{}/elasticsearch-hadoop-{}".format(
    os.getenv("ES_HADOOP_HOME", ""), os.getenv("ES_HADOOP_VERSION", "")
)
HADOOP_HOST = os.getenv("HADOOP_HOST", "spark-service")
# Searches same folders as load_json above

try:
    MAPPING_FILE = find_paths("etlMapping.yaml", "tube")[0]
except:
    MAPPING_FILE = None

try:
    USERYAML_FILE = find_paths("user.yaml", "tube")[0]
except IndexError:
    USERYAML_FILE = None
PROJECT_TO_RESOURCE_PATH = get_resource_paths_from_yaml(USERYAML_FILE)

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[1]")  # 'spark-service'
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "512m")
APP_NAME = "Gen3 ETL"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--jars {}/dist/elasticsearch-spark-20_2.11-{}.jar pyspark-shell".format(
    ES_HADOOP_HOME_BIN, ES_HADOOP_VERSION
)
os.environ["HADOOP_CLIENT_OPTS"] = os.getenv("HADOOP_CLIENT_OPTS", "")

from pwd import getpwnam

try:
    uid = getpwnam("gen3").pw_uid
    gid = getpwnam("gen3").pw_gid
    os.chown("/result", uid, gid)
    os.chmod("/result", 700)
except Exception as e:
    print(f"Could not update access: {e}")
