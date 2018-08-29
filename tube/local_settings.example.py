import os

DB_HOST = ''
DB_DATABASE = ''
DB_USERNAME = ''
DB_PASSWORD = ''
JDBC = 'jdbc:postgresql://{}/{}'.format(DB_HOST, DB_DATABASE)
PYDBC = 'postgresql://{}:{}@{}:5432/{}'.format(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_DATABASE)
DICTIONARY_URL = os.getenv('DICTIONARY_URL', 'https://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json')
ES_URL = os.getenv("ES_URL", "localhost")

HDFS_DIR = '/result'
# Three modes: Test, Dev, Prod
RUNNING_MODE = 'Dev'
SPARK_MASTER = 'local[1]'
PARALLEL_JOBS = 1

ES = {
    "es.nodes": ES_URL,
    "es.port": '9200',
    "es.resource": 'etl',
    "es.input.json": "yes"
}

HADOOP_HOME = os.getenv("HADOOP_HOME", "/usr/local/Cellar/hadoop/3.1.0/libexec/")
JAVA_HOME = os.getenv("JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home")
HADOOP_URL = os.getenv("HADOOP_URL", "hdfs://localhost:9000")
ES_HADOOP_VERSION = os.getenv("ES_HADOOP_VERSION", "")
ES_HADOOP_HOME_BIN = '{}/elasticsearch-hadoop-{}'.format(os.getenv("ES_HADOOP_HOME", ""), os.getenv("ES_HADOOP_VERSION", ""))
HADOOP_HOST = os.getenv("HADOOP_HOST", "spark")
MAPPING_FILE = os.getenv("MAPPING_FILE", "")
