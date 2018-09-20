import json

from elasticsearch import Elasticsearch, client

from tube.utils import generate_mapping
from tube.spark.plugins import post_process_plugins, add_auth_resource_path_mapping

from tube.spark.spark_base import SparkBase


def json_export(x):
    x[1]['node_id'] = x[0]
    return (x[0], json.dumps(x[1]))


class ESWriter(SparkBase):
    def __init__(self, sc, config):
        super(ESWriter, self).__init__(sc, config)
        self.es_config = self.config.ES

    def create_index(self, mapping, index):
        """
        :param mapping: mapping for index
        :return:
        """
        es_hosts = self.es_config['es.nodes']
        es_port = self.es_config['es.port']
        es_resource = index

        es = Elasticsearch([{'host': es_hosts, 'port': es_port}])
        indices = client.IndicesClient(es)

        print('Create index: {}'.format(es_resource))
        if not indices.exists(index=es_resource):
            print(es_resource)
            indices.create(index=index, body=mapping)
        return

    def write_df(self, df, index, doc_name, types):
        for plugin in post_process_plugins:
            df = df.map(lambda x: plugin(x))

        types = add_auth_resource_path_mapping(types)
        mapping = generate_mapping(doc_name, types)
        self.create_index(mapping, index)

        df = df.map(lambda x: json_export(x))
        es_config = self.es_config
        es_config['es.resource'] = index + '/{}'.format(doc_name)
        df.saveAsNewAPIHadoopFile(path='-',
                                  outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat',
                                  keyClass='org.apache.hadoop.io.NullWritable',
                                  valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
                                  conf=es_config)
