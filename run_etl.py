import argparse
import tube.settings as config
from tube.importers.sql_to_hdfs import SqlToHDFS
from tube.formatters import BaseFormatter
from tube.etl import make_spark_context as etl_make_spark_context
import tube.etl.indexers.interpreter as interpreter
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
    translators = interpreter.create_translators(sc, config)
    interpreter.run_transform(translators)
    sc.stop()


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

    args = parser.parse_args()
    config.RUNNING_MODE = args.config
    return args


def main():
    args = config_by_args()

    es_hosts = config.ES['es.nodes']
    es_port = config.ES['es.port']
    es = Elasticsearch([{'host': es_hosts, 'port': es_port}])
    index_names = interpreter.get_index_names(config)

    if check_to_run_etl(es, index_names):
        if args.step == "import" or args.step == "all":
            run_import()
        if args.step == "transform" or args.step == "all":
            run_transform()


if __name__ == '__main__':
    # Execute Main functionality
    main()
