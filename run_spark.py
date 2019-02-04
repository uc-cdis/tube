import tube.settings as config
from run_etl import run_transform
from elasticsearch import Elasticsearch


if __name__ == '__main__':
    # Execute Main functionality
    es_hosts = config.ES['es.nodes']
    es_port = config.ES['es.port']
    es = Elasticsearch([{'host': es_hosts, 'port': es_port}])

    run_transform()

