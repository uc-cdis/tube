import os

JDBC = 'jdbc:postgresql://localhost/bpa_graph'
DB_USERNAME = ''
DB_PASSWORD = ''

PYDBC = 'postgresql://test:test@localhost:5432/bpa_graph'
DICTIONARY_URL = 'link to dictionary'

HDFS_DIR = '/result'
# Three modes: Test, Dev, Prod
RUNNING_MODE = 'Dev'
SPARK_MASTER = 'local[1]'
PARALLEL_JOBS = 1

ES = {
    "es.nodes": 'localhost',
    "es.port": '9200',
    "es.resource": 'etl',
    "es.input.json": "yes",
    "es.mapping.id": "submitter_id"
}

HADOOP_HOME = os.getenv("HADOOP_HOME", "/usr/local/Cellar/hadoop/3.1.0/libexec/")
JAVA_HOME = os.getenv("JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home")
