import json
import re

from elasticsearch import helpers


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


class Versioning(object):
    def __init__(self, es):
        self.es = es
        # Reset status, putting here to declare all properties.
        self.old_indices_to_forget = None
        self.last_index = None
        self.target_version = None
        self.backup_index = None

    def reset_status(self):
        """
        Reset all the running status used to write the data frame
        :return:
        """
        self.old_indices_to_forget = None
        self.last_index = None
        self.target_version = None
        self.backup_index = None

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
            index_name = list(self.es.indices.get_alias(name=backup_alias).keys())[0]
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
        self.es.indices.put_alias(index=backup_index, name=index_alias_name)
        self.es.indices.delete(index_to_backup)
        self.es.indices.put_alias(index=backup_index, name=backup_alias)
        if backup_version > 0:
            self.es.indices.delete_alias(index=old_backup, name=backup_alias)
        return backup_index

    def clean_up(self, index):
        """
        Clean the alias from the old/backup index
        :param index: the name/alias of the index to be cleaned
        :return:
        """
        if self.old_indices_to_forget is not None:
            for old_index_to_forget in self.old_indices_to_forget:
                self.es.indices.delete_alias(index=old_index_to_forget, name=index)
        if self.backup_index is not None:
            self.es.indices.delete_alias(index=self.backup_index, name=index)

    def get_index_version(self, index):
        """
        Get the next version of index that number will be added as a suffix into index name
        :param index: original name/alias of index
        :return: -1: if the alias does not exist while the index with that name exists
                 0: if the alias does not exists,
                 n: (n > 0) if there exists the alias and n is the new increased version.
        """
        alias_existed = self.es.indices.exists_alias(name=index)
        if not alias_existed:
            if self.es.indices.exists(index=index):
                return -1
            return 0

        self.old_indices_to_forget = list(self.es.indices.get_alias(name=index).keys())
        self.last_index = sorted(self.old_indices_to_forget)[-1]
        res = re.match('.*?([0-9]+)$', self.last_index)
        if res is not None:
            return int(res.group(1)) + 1
        return 0

    def backup_old_index(self, index):
        """
        Create an empty versioned index.
        :param index: the name/alias of index
        :return:
        """
        self.target_version = self.get_index_version(index)
        if self.target_version == -1:
            self.backup_index = self.do_backup_index(index, index)
            self.target_version = 0

        versioned_index_name = get_index_name(index, self.target_version)
        if self.es.indices.exists(index=versioned_index_name):
            self.backup_index = self.do_backup_index(index, versioned_index_name)
        return versioned_index_name

    def create_new_index(self, mapping, versioned_index_name):
        self.es.indices.create(index=versioned_index_name, body=mapping)
        return versioned_index_name

    def putting_new_version_tag(self, index_to_write, index_name):
        self.es.indices.put_alias(index=index_to_write, name=index_name)
        self.clean_up(index_name)
