import argparse
import tube.settings as config

from tube.utils import get_sql_to_hdfs_config
from tube.importers.sql_to_hdfs import get_all_tables, generate_import
from cdislogging import get_logger

logger = get_logger(__name__)


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

    stream = generate_import(
        get_all_tables(config.PYDBC),
        get_sql_to_hdfs_config(config.__dict__)
    )
    for line in stream:
        print(line)


if __name__ == '__main__':
    # Execute Main functionality
    main()
