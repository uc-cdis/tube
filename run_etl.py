import argparse
import tube.settings as config
import tube.enums as enums
import tube.etl.indexers.interpreter as interpreter
import traceback
from tube.importers.sql_to_hdfs import SqlToHDFS
from tube.formatters import BaseFormatter
from tube.utils.spark import make_spark_context
from tube.etl.outputs.es.timestamp import check_to_run_etl
from elasticsearch import Elasticsearch
from py4j.protocol import Py4JJavaError


def run_import():
    """
    Define the spark context and parse agruments into config
    """
    try:
        print("!!! Running import !!!")
        sql_to_hdfs = SqlToHDFS(config, BaseFormatter())
        stream = sql_to_hdfs.generate_import_all_tables()
        if stream is None:
            print("WARNING: No data to import!!")
            return
        for line in stream:
            print(line.rstrip())
    except Exception as ex:
        print("ERROR when running import to hadoop")
        print(traceback.format_exc())
        raise


def run_transform():
    print("!!! Running transform !!!")
    try:
        sc = make_spark_context(config)
        translators = interpreter.create_translators(sc, config)
        interpreter.run_transform(translators)
    except Py4JJavaError as py4J_ex:
        print("ERROR when connecting to spark. Please roll spark")
        print(py4J_ex)
        print(traceback.format_exc())
    except Exception as ex:
        print("ERROR when running transformation")
        print(ex)
        print(traceback.format_exc())
        raise
    finally:
        sc.stop()


def config_by_args():
    """
    Define the spark context and parse agruments into config
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        help="The configuration set to run with",
        type=str,
        choices=enums.RUNNING_MODE_CHOICES,
        default="Dev",
    )
    parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )
    parser.add_argument(
        "-s",
        "--step",
        help="The step to run with",
        type=str,
        choices=enums.RUNNING_STEP_CHOICES,
        default="all",
    )
    parser.add_argument(
        "-f",
        "--force",
        help="Force ETL run when there is no new data",
        action="store_true",
    )

    args = parser.parse_args()
    config.RUNNING_MODE = args.config
    return args


def main():
    args = config_by_args()

    es_hosts = config.ES["es.nodes"]
    es_port = int(config.ES["es.port"])
    es_schema = "https" if es_port == 443 or config.ES["es.use_ssl"] else "http"
    es_config = {
        "host": es_hosts,
        "port": es_port,
        "schema": es_schema,
    }
    if config.ES["es.http_auth.username"] is not None and config.ES["es.http_auth.password"] is not None:
        es_config["http_auth"] = (config.ES["es.http_auth.username"], config.ES["es.http_auth.password"])

    es = Elasticsearch([es_config])
    index_names = interpreter.get_index_names(config)

    if args.force or check_to_run_etl(es, index_names):
        if (
            args.step == enums.RUNNING_STEP_IMPORT
            or args.step == enums.RUNNING_STEP_ALL
        ):
            run_import()
        if (
            args.step == enums.RUNNING_STEP_TRANSFORM
            or args.step == enums.RUNNING_STEP_ALL
        ):
            run_transform()
    else:
        print("Nothing's new")


if __name__ == "__main__":
    # Execute Main functionality
    main()
