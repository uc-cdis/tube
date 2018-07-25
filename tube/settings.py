from cdislogging import get_logger

logger = get_logger(__name__)
try:
    # Import everything from ``local_settings``, if it exists.
    from local_settings import *
except ImportError:
    # If it doesn't, look in ``/var/www/tube``.
    try:
        import imp
        imp.load_source('local_settings', '/var/www/tube/local_settings.py')
        print('finished importing')
    except IOError:
        logger.warn("local_settings is not found")

HDFS_DIR = '/result'
APP_NAME = 'Gen3 ETL'
# Three modes: Test, Dev, Prod
RUNNING_MODE = 'Dev'
SPARK_MASTER = 'local[1]'
LIST_TABLES_FILES = 'tables.txt'
PARALLEL_JOBS = 1
LINK_TO_ES_HADOOP_JAR = '/Users/thanhnd/Workspace/es/elasticsearch-hadoop-6.3.0/dist/elasticsearch-spark-20_2.11-6.3.0.jar pyspark-shell'
