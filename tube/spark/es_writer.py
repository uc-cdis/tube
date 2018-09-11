import json

from elasticsearch import Elasticsearch, client

from tube.spark.plugins import post_process_plugins
from tube.spark.spark_base import SparkBase


def json_export(x):
    x[1]['node_id'] = x[0]
    return (x[0], json.dumps(x[1]))


class ESWriter(SparkBase):
    def __init__(self, sc, config):
        super(ESWriter, self).__init__(sc, config)
        self.es_config = self.config.ES

    def get_types(self, df):
        """
        Gather the list of JSON field with the strings values from the first element of the RDD.

        :param df: input PipelinedRDD
        :return: list of JSON fields with strings
        """
        item = df.first()
        json_data = item[1]
        string_fields = []
        # tuple of Python types that needs to be "keyword" in Elasticsearch
        keyword_types = (basestring,  # for string and "basestring" subclasses
                         type(None),  # "None" values
                         tuple, list, dict, set  # all container type
                         )
        for key, value in json_data.items():
            if isinstance(value, keyword_types):
                string_fields.append(key)

        return string_fields

    def generate_mapping(self, string_fields, doc_name):
        """
        :param string_fields: list of field to have "keyword" type
        :return: JSON with proper mapping to be used in Elasticsearch
        """
        keyword_type = {
            "type": "keyword"
        }
        properties = {string_field: keyword_type for string_field in string_fields}

        # explicitly mapping for add "node_id"
        properties['node_id'] = keyword_type

        mapping = {"mappings": {
            doc_name: {"properties": properties}
        }}
        return mapping

    def create_index(self, mapping, doc_name):
        """
        :param mapping: mapping for index
        :return:
        """
        es_hosts = self.es_config["es.nodes"]
        es_port = self.es_config["es.port"]
        es_resource = self.es_config["es.resource"]

        es = Elasticsearch([{"host": es_hosts, "port": es_port}])
        indices = client.IndicesClient(es)

        if not indices.exists(index=es_resource):
            indices.create(index=es_resource, body=mapping)
        return

    def precreate_index_with_mapping(self, df, doc_name):
        string_fields = self.get_types(df)
        mapping = self.generate_mapping(string_fields, doc_name)

        self.create_index(mapping, doc_name)
        return

    def write_df(self, df, doc_name):
        for plugin in post_process_plugins:
            df = df.map(lambda x: plugin(x))

        self.precreate_index_with_mapping(df, doc_name)

        df = df.map(lambda x: json_export(x))
        es_config = self.es_config
        es_config['es.resource'] = es_config['es.resource'] + '/{}'.format(doc_name)
        # df.saveAsTextFile('{}/output'.format(self.config.HDFS_DIR))
        df.saveAsNewAPIHadoopFile(path='-',
                                  outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                  keyClass="org.apache.hadoop.io.NullWritable",
                                  valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                  conf=es_config)
