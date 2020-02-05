import argparse
import tube.settings as config
import tube.etl.indexers.interpreter as interpreter
import traceback
from tube.importers.sql_to_hdfs import SqlToHDFS
from tube.formatters import BaseFormatter
from tube.utils.spark import make_spark_context
from tube.etl.outputs.es.timestamp import check_to_run_etl
from elasticsearch import Elasticsearch


def run_import():
    '''
    Define the spark context and parse agruments into config
    '''
    try:
        sql_to_hdfs = SqlToHDFS(config, BaseFormatter())
        stream = sql_to_hdfs.generate_import_all_tables()
        if stream is None:
            return
        for line in stream:
            print(line.rstrip())
    except Exception as ex:
        print('ERROR when running import to hadoop')
        print(traceback.format_exc())


def run_transform():
    try:
        sc = make_spark_context(config)
        translators = interpreter.create_translators(sc, config)
        interpreter.run_transform(translators)
        sc.stop()
    except Exception as ex:
        print('ERROR when running transformation')
        print(traceback.format_exc())


def config_by_args():
    '''
    Define the spark context and parse agruments into config
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='The configuration set to run with',
                        type=str,
                        choices=['Test', 'Dev', 'Prod'],
                        default='Dev')
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity",
                        action="store_true")
    parser.add_argument('-s', '--step',
                        help='The step to run with',
                        type=str,
                        choices=['import', 'transform', 'all'],
                        default='all')
    parser.add_argument('-f', '--force',
                        help='Force ETL run when there is no new data',
                        action="store_true")

    args = parser.parse_args()
    config.RUNNING_MODE = args.config
    return args


def main():
    args = config_by_args()

    es_hosts = config.ES['es.nodes']
    es_port = config.ES['es.port']
    es = Elasticsearch([{'host': es_hosts, 'port': es_port}])
    index_names = interpreter.get_index_names(config)

    if args.force or check_to_run_etl(es, index_names):
        if args.step == "import" or args.step == "all":
            run_import()
        if args.step == "transform" or args.step == "all":
            run_transform()
    else:
        print("Nothing's new")


if __name__ == '__main__':
    # Execute Main functionality
    main()
