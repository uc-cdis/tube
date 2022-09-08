import json

from elasticsearch import Elasticsearch

from tube.etl.outputs.es.timestamp import (
    putting_timestamp,
    get_latest_utc_transaction_time,
)
from tube.etl.outputs.es.versioning import Versioning
from tube.etl.plugins import (
    post_process_plugins,
    add_auth_resource_path_mapping,
    post_process_plugins_on_dataframe,
)
from tube.etl.spark_base import SparkBase
from tube.utils.general import get_node_id_name


def json_export(x, doc_type):
    x[1][get_node_id_name(doc_type)] = x[0]
    x[1]["node_id"] = x[0]  # redundant field for backward compatibility with arranger
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

    def get_es(self):
        """
        Create ElasticSearch instance
        :return:
        """
        es_hosts = self.es_config["es.nodes"]
        es_port = self.es_config["es.port"]
        return Elasticsearch([{"host": es_hosts, "port": es_port}])

    def write_to_new_index(self, df, index, doc_type):
        df = df.map(lambda x: json_export(x, doc_type))
        es_config = self.es_config
        es_config["es.resource"] = index + "/{}".format(doc_type)
        df.saveAsNewAPIHadoopFile(
            path="-",
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_config,
        )

    def write_df_to_new_index(self, df, index, doc_type):
        es_config = self.es_config
        es_config["es.resource"] = index
        df.coalesce(1).write.format("org.elasticsearch.spark.sql").option(
            "es.nodes", es_config["es.nodes"]
        ).option("es.port", es_config["es.port"]).option(
            "es.nodes.wan.only", "true"
        ).option(
            "es.nodes.discovery", es_config["es.nodes.discovery"]
        ).option(
            "es.nodes.data.only", es_config["es.nodes.data.only"]
        ).option(
            "es.nodes.client.only", es_config["es.nodes.client.only"]
        ).option(
            "es.resource", es_config["es.resource"]
        ).save(
            index
        )

    def create_guppy_array_config(self, parser):  # etl_index_name, types, array_types):
        """
        Create index with Guppy configuration for array fields
        :param parser:
        """
        etl_index_name = parser.name
        types = parser.types.get(parser.doc_type).get("properties")
        array_types = parser.array_types
        index = "{}-array-config".format(etl_index_name)
        alias = "{}_array-config".format(etl_index_name.split("_")[0])

        mapping = {
            "mappings": {
                "type_name": {
                    "properties": {
                        "timestamp": {"type": "date"},
                        "array": {"type": "keyword"},
                    }
                }
            }
        }

        latest_transaction_time = get_latest_utc_transaction_time()

        doc = {
            "timestamp": latest_transaction_time,
            "array": ["{}".format(k) for k in array_types],
        }

        try:
            self.reset_status()
            index_to_write = self.versioning.create_new_index(
                mapping, self.versioning.get_next_index_version(index)
            )
            self.es.index(index_to_write, "_doc", id=etl_index_name, body=doc)
            self.versioning.putting_new_version_tag(index_to_write, index)
            self.versioning.putting_new_version_tag(index_to_write, alias)
            putting_timestamp(self.es, index_to_write)
            self.reset_status()
        except Exception as e:
            print(e)
            raise

    def write_dataframe(self, df, index, doc_type, types):
        self.reset_status()
        types = add_auth_resource_path_mapping(doc_type, types)

        for plugin in post_process_plugins_on_dataframe:
            df = plugin(df)

        index_to_write = self.versioning.create_new_index(
            {"mappings": types.get(doc_type)}, self.versioning.get_next_index_version(index)
        )
        self.write_to_es(
            df, index_to_write, index, doc_type, self.write_df_to_new_index
        )
        return index_to_write

    def write_to_es(self, df, index_to_write, index, doc_type, fn):
        """
        Function to write the data frame to ElasticSearch
        :param df: data frame to be written
        :param index_to_write: exact name of index
        :param index: name of the index (alias)
        :param doc_type: document type's name
        :param types:
        :return:
        """
        try:
            fn(df, index_to_write, doc_type)
            self.versioning.putting_new_version_tag(index_to_write, index)
            putting_timestamp(self.es, index_to_write)
            self.reset_status()
        except Exception as e:
            print(e)
            raise
