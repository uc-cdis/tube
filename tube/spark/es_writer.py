import json
import re

from elasticsearch import Elasticsearch, helpers

from tube.utils import generate_mapping
from tube.spark.plugins import post_process_plugins, add_auth_resource_path_mapping
from tube.spark.spark_base import SparkBase


def json_export(x):
    x[1]['node_id'] = x[0]
    return (x[0], json.dumps(x[1]))


def get_index_name(index, version):
    return '{}_{}'.format(index, version)


def get_backup_index_name(index, version):
    return '{}_{}'.format(version, index)


def get_backup_alias(index):
    return '{}_backup'.format(index)


def get_backup_version(index_name):
    res = re.match('^[0-9]+', index_name)
    if res is not None:
        return int(res.group()) + 1
    return 0


class ESWriter(SparkBase):
    def __init__(self, sc, config):
        super(ESWriter, self).__init__(sc, config)
        self.es_config = self.config.ES
        self.reset_status()
        self.es = self.get_es()

    def reset_status(self):
        """
        Reset all the running status used to write the data frame
        :return:
        """
        self.conflict_index = None
        self.old_index_to_forget = None
        self.target_version = None
        self.backup_index = None

    def get_es(self):
        """
        Create ElasticSearch instance
        :return:
        """
        es_hosts = self.es_config['es.nodes']
        es_port = self.es_config['es.port']
        return Elasticsearch([{'host': es_hosts, 'port': es_port}])

    def get_backup_info(self, index):
        """
        Retrieve the information of backing up the index
        :param index: name/alias of the index to to backed up
        :return:
        """
        backup_alias = get_backup_alias(index)
        index_name = None
        backup_version = 0
        if self.es.indices.exists_alias(name=backup_alias):
            index_name = self.es.indices.get_alias(name=backup_alias).keys()[0]
            backup_version = get_backup_version(index_name)
        backup_index = get_backup_index_name(index, backup_version)
        return backup_alias, backup_version, backup_index, index_name

    def do_backup_index(self, index_alias_name, index_to_backup):
        """
        Backup the existing index having name as index_to_backup
        :param index_alias_name: alias to put into the new index to maintain the availability
        :param index_to_backup: name of index
        :return:
        """
        backup_alias, backup_version, backup_index, old_backup = self.get_backup_info(index_to_backup)
        # create index to store the existing index
        self.es.indices.create(index=backup_index)
        helpers.reindex(self.es, source_index=index_to_backup,
                        target_index=backup_index)
        self.es.indices.delete(index_to_backup)
        self.es.indices.put_alias(index=backup_index, name=backup_alias)
        self.es.indices.put_alias(index=backup_index, name=index_alias_name)
        if backup_version > 0:
            self.es.indices.delete_alias(index=old_backup, name=backup_alias)
        return backup_index

    def clean_up(self, index):
        """
        Clean the alias from the old/backup index
        :param index: the name/alias of the index to be cleaned
        :return:
        """
        if self.old_index_to_forget is not None:
            self.es.indices.delete_alias(index=self.old_index_to_forget, name=index)
        if self.backup_index is not None:
            self.es.indices.delete_alias(index=self.backup_index, name=index)
        if self.conflict_index is not None:
            self.es.indices.delete_alias(index=self.conflict_index, name=index)

    def create_new_index(self, index, version, mapping):
        versioned_index_name = get_index_name(index, version)
        if self.es.indices.exists(index=versioned_index_name):
            self.conflict_index = versioned_index_name
            self.backup_index = self.do_backup_index(index, self.conflict_index)
        self.es.indices.create(index=versioned_index_name, body=mapping)
        return versioned_index_name

    def get_index_version(self, index):
        """
        Get the next version of index that number will be added as a suffix into index name
        :param index: original name/alias of index
        :return:
        """
        alias_existed = self.es.indices.exists_alias(name=index)
        if not alias_existed:
            if self.es.indices.exists(index=index):
                return -1
            return 0

        self.old_index_to_forget = self.es.indices.get_alias(name=index).keys()[0]
        res = re.match('.*?([0-9]+)$', self.old_index_to_forget)
        if res is not None:
            return int(res.group(1)) + 1
        return 0

    def create_index(self, mapping, index):
        """
        Create an empty versioned index.
        :param mapping: mapping for index
        :param index: the name/alias of index
        :return:
        """
        self.target_version = self.get_index_version(index)
        if self.target_version == -1:
            self.backup_index = self.do_backup_index(index, index)
            self.target_version = 0
        return self.create_new_index(index, self.target_version, mapping)

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
            # self.reset_status()
            for plugin in post_process_plugins:
                df = df.map(lambda x: plugin(x))

            # types = add_auth_resource_path_mapping(types)
            # mapping = generate_mapping(doc_type, types)
            # current_index = self.create_index(mapping, index)
            #
            # df = df.map(lambda x: json_export(x))
            # es_config = self.es_config
            # es_config['es.resource'] = current_index + '/{}'.format(doc_type)
            # df.saveAsNewAPIHadoopFile(path='-',
            #                           outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat',
            #                           keyClass='org.apache.hadoop.io.NullWritable',
            #                           valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
            #                           conf=es_config)
            # self.es.indices.put_alias(index=current_index, name=index)
            # self.clean_up(index)
            # self.reset_status()
        except Exception as e:
            print(e)
