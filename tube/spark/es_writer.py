import json
from tube.spark.spark_base import SparkBase


def json_export(x):
    x[1]['node_id'] = x[0]
    return (x[0], json.dumps(x[1]))


class ESWriter(SparkBase):
    def __init__(self, sc, config):
        super(ESWriter, self).__init__(sc, config)
        self.es_config = self.config.ES

    def write_df(self, df, doc_name):
        df = df.map(lambda x: json_export(x))
        es_config = self.es_config
        es_config['es.resource'] = es_config['es.resource'] + '/{}'.format(doc_name)
        df.saveAsNewAPIHadoopFile(path='-',
                                  outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                  keyClass="org.apache.hadoop.io.NullWritable",
                                  valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                  conf=es_config)
