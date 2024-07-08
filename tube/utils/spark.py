import os

from pyspark import SparkConf, SparkContext
from pyspark.sql.types import (
    StringType,
    ArrayType,
    LongType,
    FloatType,
)

import tube.settings as config
import tube.enums as enums


def make_spark_context(tube_config):
    """
    Makes a spark and sqlContext
    """
    conf = (
        SparkConf()
        .set("spark.executor.memory", tube_config.SPARK_EXECUTOR_MEMORY)
        .set("spark.driver.memory", tube_config.SPARK_DRIVER_MEMORY)
        .set("spark.python.profile", "false")
        .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/spark/conf/log4j.properties")
        # .set("spark.submit.pyFiles", "/tube")
        .setAppName(config.APP_NAME)
    )
    if tube_config.RUNNING_MODE == enums.RUNNING_MODE_DEV:
        # We should only use the value of `config.spark_master` in
        # a test context. Production runs need to set the Spark Master
        # to 'yarn'. This is done in the arguments to `spark-submit`.
        conf = conf.setMaster(tube_config.SPARK_MASTER)
    sc = SparkContext.getOrCreate(conf=conf)

    # Configure logging
    log4j = sc._jvm.org.apache.log4j
    if config.LOG_LEVEL == "WARN":
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
    else:
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.INFO)

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


def save_rdds(df, path, sc):
    fs, opath, sc = get_hdfs_file_handler(sc)
    fs.delete(opath(path), True)
    df.saveAsPickleFile(path)


def get_all_files_from_hdfs(path, sc):
    fs, opath, sc = get_hdfs_file_handler(sc)
    status = fs.listStatus(opath(path))
    files = []
    for p in status:
        files.append(p.getPath().toString())
    return files


def get_all_files(path, sc):
    if config.RUNNING_MODE.lower() == enums.RUNNING_MODE_TEST.lower():
        return [os.path.abspath(os.path.join(path, f)) for f in os.listdir(path)]
    return get_all_files_from_hdfs(path, sc)


def get_hadoop_type(prop):
    if prop.fn is not None and prop.fn in ["list", "set"]:
        return ArrayType(StringType())
    if prop.type == (float,):
        return FloatType()
    if prop.type == (str,):
        return StringType()
    if prop.type == (int,):
        return LongType()
    return StringType()


def get_hadoop_simple_type(p_type):
    if p_type is float:
        return FloatType()
    if p_type is str:
        return StringType()
    if p_type is int:
        return LongType()
    return StringType()


def get_hadoop_type_ignore_fn(prop):
    if prop.type is None:
        return StringType()
    if prop.type[0] is list:
        if len(prop.type) > 1:
            return ArrayType(get_hadoop_simple_type(prop.type[1]))
        return ArrayType(StringType())
    return get_hadoop_simple_type(prop.type[0])
