from pyspark import SparkConf, SparkContext

import tube.settings as config


def make_spark_context(config):
    """
    Makes a spark and sqlContext
    """
    conf = (
        SparkConf()
        .set("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY)
        .set("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .setAppName(config.APP_NAME)
    )
    if config.RUNNING_MODE == "Dev":
        # We should only use the value of `config.spark_master` in
        # a test context. Production runs need to set the Spark Master
        # to 'yarn'. This is done in the arguments to `spark-submit`.
        conf = conf.setMaster(config.SPARK_MASTER)
    sc = SparkContext(conf=conf, pyFiles=[])

    # Configure logging
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)

    return sc


def get_hdfs_file_handler(sc=None):
    if sc is None:
        sc = make_spark_context(config)
    uri = sc._gateway.jvm.java.net.URI
    opath = sc._gateway.jvm.org.apache.hadoop.fs.Path
    file_system = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = file_system.get(uri(config.HADOOP_URL), sc._jsc.hadoopConfiguration())
    return fs, opath, sc


def make_sure_hdfs_path_exist(path, sc=None):
    fs, opath, sc = get_hdfs_file_handler(sc)
    fs.delete(opath(path), True)
    fs.mkdirs(opath(path))
    sc.stop()
    return path


def save_rdd_of_dataframe(df, path, sc):
    fs, opath, sc = get_hdfs_file_handler(sc)
    fs.delete(opath(path), True)
    df.write.parquet(path)


def get_all_files(path, sc):
    fs, opath, sc = get_hdfs_file_handler(sc)
    status = fs.listStatus(opath(path))
    files = []
    for p in status:
        files.append(p.getPath().toString())
    return files
