import argparse
import tube.settings as config

from tube.spark import make_spark_context
from tube.spark.indexers.interpreter import Interpreter
from tube.spark.es_writer import ESWriter


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

    args = parser.parse_args()

    config.RUNNING_MODE = args.config

    sc = make_spark_context(config)
    writer = ESWriter(sc, config)
    etl = Interpreter(sc, writer, config)
    etl.run_etl()

    sc.stop()


if __name__ == '__main__':
    # Execute Main functionality
    main()
