import argparse
import tube.settings as config
from tube.importers.sql_to_hdfs import SqlToHDFS
from tube.formatters import BaseFormatter


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

    sql_to_hdfs = SqlToHDFS(config, BaseFormatter())
    stream = sql_to_hdfs.generate_import_all_tables()
    for line in stream:
        print(line)


if __name__ == '__main__':
    # Execute Main functionality
    main()
