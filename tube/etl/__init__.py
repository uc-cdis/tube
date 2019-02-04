import os
from pyspark import SparkConf, SparkContext


def make_spark_context(config):
    '''
    Makes a spark and sqlContext
    '''
    conf = SparkConf() \
        .set('spark.executor.memory', '2g') \
        .set('spark.driver.memory', '2g') \
        .setAppName(config.APP_NAME)
    if config.RUNNING_MODE == 'Dev':
        # We should only use the value of `config.spark_master` in
        # a test context. Production runs need to set the Spark Master
        # to 'yarn'. This is done in the arguments to `spark-submit`.
        conf = conf.setMaster(config.SPARK_MASTER)

    sc = SparkContext(conf=conf, pyFiles=[])

    # Configure logging
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)

    return sc
