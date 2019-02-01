import argparse
import tube.settings as config
from tube.importers.sql_to_hdfs import SqlToHDFS
from tube.formatters import BaseFormatter
from tube.etl import make_spark_context as etl_make_spark_context
from tube.etl.indexers.interpreter import Interpreter
from tube.etl.outputs.es.writer import Writer
from tube.etl.outputs.es.timestamp import check_to_run_etl
from elasticsearch import Elasticsearch


def run_import():
    '''
    Define the spark context and parse agruments into config
    '''
    sql_to_hdfs = SqlToHDFS(config, BaseFormatter())
    stream = sql_to_hdfs.generate_import_all_tables()
    if stream is None:
        return
    for line in stream:
        print(line.rstrip())


def run_transform():
    sc = etl_make_spark_context(config)
    writer = Writer(sc, config)
    etl = Interpreter(sc, writer, config)
    etl.run_transform()
    sc.stop()


def main():
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

    args = parser.parse_args()
    config.RUNNING_MODE = args.config

    es_hosts = config.ES['es.nodes']
    es_port = config.ES['es.port']
    es = Elasticsearch([{'host': es_hosts, 'port': es_port}])

    if check_to_run_etl(es, "etl"):
        if args.step == "import" or args.step == "all":
            run_import()
        if args.step == "transform" or args.step == "all":
            run_transform()


if __name__ == '__main__':
    # Execute Main functionality
    main()
