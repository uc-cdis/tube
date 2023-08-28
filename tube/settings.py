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
# Read from env var or fall back to conf_data
DB_HOST = os.getenv("DB_HOST") or conf_data.get("db_host", "localhost")
DB_PORT = os.getenv("DB_PORT") or conf_data.get("db_port", "5432")
DB_DATABASE = os.getenv("DB_DATABASE") or conf_data.get("db_database", "sheepdog")
DB_USERNAME = os.getenv("DB_USERNAME") or conf_data.get("db_username", "peregrine")
DB_PASSWORD = os.getenv("DB_PASSWORD") or conf_data.get("db_password", "unknown")
DB_USE_SSL = False
# bool(os.getenv("DB_USE_SSL", False)) or conf_data.get(
#     "db_use_ssl", False
# )  # optional property to db_use_ssl
JDBC = "jdbc:postgresql://{}:{}/{}".format(DB_HOST, DB_PORT, DB_DATABASE)
PYDBC = "postgresql://{}:{}@{}:{}/{}".format(
    DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE
)
DICTIONARY_URL = os.getenv(
    "DICTIONARY_URL",
    "https://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json",
)
ES_URL = os.getenv("ES_URL", "esproxy-service")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USE_SSL = bool(os.getenv("ES_USE_SSL", False))
ES_AUTH_USERNAME = os.getenv("ES_AUTH_USERNAME")
ES_AUTH_PASSWORD = os.getenv("ES_AUTH_PASSWORD")

HDFS_DIR = "/result"
# Three modes: Test, Dev, Prod
RUNNING_MODE = os.getenv("RUNNING_MODE", enums.RUNNING_MODE_DEV)  # 'Prod' or 'Dev'

PARALLEL_JOBS = 1
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

ES = {
    "es.nodes": ES_URL,
    "es.port": ES_PORT,
    "es.use_ssl": ES_USE_SSL,
    "es.http_auth.username": ES_AUTH_USERNAME,
    "es.http_auth.password": ES_AUTH_PASSWORD,
    "es.input.json": "yes",
    "es.nodes.client.only": "false",
    "es.nodes.discovery": "false",
    "es.nodes.data.only": "false",
    "es.nodes.wan.only": "true",
}

HADOOP_HOME = os.getenv("HADOOP_HOME", "/usr/local/Cellar/hadoop/3.1.0/libexec/")
JAVA_HOME = os.getenv(
    "JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home"
)
HADOOP_URL = os.getenv("HADOOP_URL", "http://spark-service:9000")
ES_HADOOP_VERSION = os.getenv("ES_HADOOP_VERSION", "")
ES_HADOOP_HOME_BIN = "{}/elasticsearch-hadoop-{}".format(
    os.getenv("ES_HADOOP_HOME", ""), os.getenv("ES_HADOOP_VERSION", "")
)
HADOOP_HOST = os.getenv("HADOOP_HOST", "spark-service")
# Searches same folders as load_json above

MAPPING_FILE = find_paths("etlMapping.yaml", "tube")[0]
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
