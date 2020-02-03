import json

from elasticsearch import Elasticsearch

from tube.etl.outputs.es.timestamp import putting_timestamp, get_latest_utc_transaction_time
from tube.etl.outputs.es.versioning import Versioning
from tube.etl.plugins import post_process_plugins, add_auth_resource_path_mapping
from tube.etl.spark_base import SparkBase


def json_export(x, doc_type):
    x[1]['{}_id'.format(doc_type)] = x[0]
    x[1]['node_id'] = x[0]  # redundant field for backward compatibility with arranger
    return (x[0], json.dumps(x[1]))


class Writer(SparkBase):
    def __init__(self, sc, config):
        super(Writer, self).__init__(sc, config)
        self.es_config = self.config.ES
        self.es = self.get_es()
        self.es.indices.get_alias()
        self.versioning = Versioning(self.es)

    def reset_status(self):
        self.versioning.reset_status()

    def generate_mapping(self, doc_name, field_types):
        """
        :param doc_name: name of the Elasticsearch document to create mapping for
        :param field_types: dictionary of field and their types
        :return: JSON with proper mapping to be used in Elasticsearch
        """
        es_type = {
            str: 'keyword',
            float: 'float',
            int: 'integer'
        }

        properties = {
            k: {
                'type': es_type[v[0]]
            } if v[0] is not str else {
                'type': es_type[v[0]],
                'fields': {
                    'analyzed': {
                        'type': 'text'
                    }
                }
            }
            for k, v in list(field_types.items())
        }

        # explicitly mapping 'node_id'
        properties['node_id'] = {'type': 'keyword'}

        mapping = {'mappings': {
            doc_name: {'properties': properties}
        }}
        return mapping

    def get_es(self):
        """
        Create ElasticSearch instance
        :return:
        """
        es_hosts = self.es_config['es.nodes']
        es_port = self.es_config['es.port']
        return Elasticsearch([{'host': es_hosts, 'port': es_port}])

    def write_to_new_index(self, df, index, doc_type):
        df = df.map(lambda x: json_export(x, doc_type))
        es_config = self.es_config
        es_config['es.resource'] = index + '/{}'.format(doc_type)
        df.saveAsNewAPIHadoopFile(path='-',
                                  outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat',
                                  keyClass='org.apache.hadoop.io.NullWritable',
                                  valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
                                  conf=es_config)

    def create_guppy_array_config(self, etl_index_name, types):
        """
        Create index with Guppy configuration for array fields
        :param etl_index_name:
        :param types:
        """
        index = '{}-array-config'.format(etl_index_name)
        alias = '{}_array-config'.format(etl_index_name.split('_')[0])

        mapping = {
            'mappings': {
                '_doc': {'properties':
                    {
                        'timestamp': {'type': 'date'},
                        'array': {'type': 'keyword'}
                    }
                }
            }
        }

        latest_transaction_time = get_latest_utc_transaction_time()

        doc = {
            'timestamp': latest_transaction_time,
            'array': ['{}'.format(k) for k, v in list(types.items()) if v[1]]
        }

        try:
            self.reset_status()
            index_to_write = self.versioning.create_new_index(mapping, self.versioning.backup_old_index(index))
            self.es.index(index_to_write, '_doc', id=etl_index_name, body=doc)
            self.versioning.putting_new_version_tag(index_to_write, index)
            self.versioning.putting_new_version_tag(index_to_write, alias)
            putting_timestamp(self.es, index_to_write)
            self.reset_status()
        except Exception as e:
            print(e)

    def write_df(self, df, index, doc_type, types):
        """
        Function to write the data frame to ElasticSearch
        :param df: data frame to be written
        :param index: name of the index
        :param doc_type: document type's name
        :param types:
        :return:
        """
        try:
            for plugin in post_process_plugins:
                df = df.map(lambda x: plugin(x))

            types = add_auth_resource_path_mapping(types)
            mapping = self.generate_mapping(doc_type, types)

            self.reset_status()
            index_to_write = self.versioning.create_new_index(mapping, self.versioning.backup_old_index(index))
            self.write_to_new_index(df, index_to_write, doc_type)
            self.versioning.putting_new_version_tag(index_to_write, index)
            putting_timestamp(self.es, index_to_write)
            self.reset_status()
        except Exception as e:
            print(e)
