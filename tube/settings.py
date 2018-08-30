from cdislogging import get_logger
from config_helper import *


logger = get_logger(__name__)

LIST_TABLES_FILES = 'tables.txt'
#LINK_TO_ES_HADOOP_JAR = '/Users/thanhnd/Workspace/es/elasticsearch-hadoop-6.3.0/dist/elasticsearch-spark-20_2.11-6.3.0.jar'

#
# Load db credentials from a creds.json file.
# See config_helper.py for paths searched for creds.json
# ex: export XDG_DATA_HOME="$HOME/.local/share"
#    and setup $XDG_DATA_HOME/.local/share/gen3/tube/creds.json
#
conf_data = load_json('creds.json', 'tube')
DB_HOST = conf_data.get( 'db_host', 'localhost' )
DB_DATABASE = conf_data.get( 'db_database', 'gdcdb' )
DB_USERNAME = conf_data.get( 'db_username', 'peregrine' )
DB_PASSWORD = conf_data.get( 'db_password', 'unknown' )
JDBC = 'jdbc:postgresql://{}/{}'.format(DB_HOST, DB_DATABASE)
PYDBC = 'postgresql://{}:{}@{}:5432/{}'.format(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_DATABASE)
DICTIONARY_URL = os.getenv('DICTIONARY_URL', 'https://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json')
ES_URL = os.getenv("ES_URL", "esproxy-service")

HDFS_DIR = '/result'
# Three modes: Test, Dev, Prod
RUNNING_MODE = os.getenv('RUNNING_MODE', 'Dev')  # 'Prod' or 'Dev'

PARALLEL_JOBS = 1

ES = {
    "es.nodes": ES_URL,
    "es.port": '9200',
    "es.resource": os.getenv('ES_INDEX_NAME', 'null'),
    "es.input.json": 'yes',
    "es.nodes.client.only": 'false',
    "es.nodes.discovery": 'false',
    "es.nodes.data.only": 'false',
    "es.nodes.wan.only": 'true'
}

if 'null' == ES['es.resource']:
  raise Exception('ES_INDEX_NAME environment not defined')

HADOOP_HOME = os.getenv('HADOOP_HOME', '/usr/local/Cellar/hadoop/3.1.0/libexec/')
JAVA_HOME = os.getenv('JAVA_HOME', '/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home')
HADOOP_URL = os.getenv('HADOOP_URL', 'http://spark-service:9000')
ES_HADOOP_VERSION = os.getenv("ES_HADOOP_VERSION", "")
ES_HADOOP_HOME_BIN = '{}/elasticsearch-hadoop-{}'.format(os.getenv("ES_HADOOP_HOME", ""), os.getenv("ES_HADOOP_VERSION", ""))
HADOOP_HOST = os.getenv("HADOOP_HOST", "spark-service")
# Searches same folders as load_json above
MAPPING_FILE = find_paths("etlMapping.yaml", 'tube')[0]
SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[1]') #'spark-service') 
APP_NAME = 'Gen3 ETL'
