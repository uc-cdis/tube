from dictionaryutils import DataDictionary, dictionary
from pyspark import SparkConf, SparkContext

import tube.settings as config


def get_sql_to_hdfs_config(config):
    return {
        'input': {
            'jdbc': config['JDBC'],
            'username': config['DB_USERNAME'],
            'password': config['DB_PASSWORD'],
        },
        'output': config['HDFS_DIR']
    }


def list_to_file(lst, file_path):
    with open(file_path, 'w') as f:
        f.write('\n'.join(lst))


def make_spark_context(config):
    '''
    Makes a spark and sqlContext
    '''
    conf = SparkConf().setAppName(config.APP_NAME)
    if config.RUNNING_MODE == 'Dev':
        # We should only use the value of `config.spark_master` in
        # a test context. Production runs need to set the Spark Master
        # to 'yarn'. This is done in the arguments to `spark-submit`.
        conf = conf.setMaster(config.SPARK_MASTER)
    sc = SparkContext(conf=conf, pyFiles=[])

    # Configure logging
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    return sc


def get_hdfs_file_handler(sc=None, hdfs=None):
    if sc is None:
        sc = make_spark_context(config)
    uri = sc._gateway.jvm.java.net.URI
    opath = sc._gateway.jvm.org.apache.hadoop.fs.Path
    file_system = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = file_system.get(uri(config.HADOOP_URL), sc._jsc.hadoopConfiguration())
    return fs, opath


def make_sure_hdfs_path_exist(path, sc=None):
    fs, opath = get_hdfs_file_handler(sc)
    if not fs.exists(opath(path)):
        fs.mkdirs(opath(path))
    return path


def init_dictionary(url):
    d = DataDictionary(url=url)
    dictionary.init(d)
    # the gdcdatamodel expects dictionary initiated on load, so this can't be
    # imported on module level
    from gdcdatamodel import models as md
    return d, md


# There are two important things of a node in a model:
# - label ( which does not include the prefix)
# - table_name: includes the prefix and in plural


def get_edge_table(models, node_label, edge_name):
    '''
    :param models: the model which node and edge belong to
    :param node_label: the label of a node
    :param edge_name: the back_ref label of an edge
    :return: (label of the source node, table name of edge specified by edge_name)
    '''
    node = models.Node.get_subclass(node_label)
    edge = getattr(node, edge_name)
    parent_label = get_node_label(models, edge.target_class.__src_class__)
    if node_label == parent_label:
        parent_label = get_node_label(models, edge.target_class.__dst_class__)
    return parent_label, edge.target_class.__tablename__


def get_child_table(models, node_name, edge_name):
    node = models.Node.get_subclass(node_name)
    edge = getattr(node, edge_name)
    src_class = edge.target_class.__src_class__
    src_label = get_node_label(models, src_class)
    if src_label != node_name:
        return models.Node.get_subclass_named(src_class).__tablename__, True
    return models.Node.get_subclass_named(edge.target_class.__dst_class__).__tablename__, False


def get_parent_name(models, node_name, edge_name):
    node = models.Node.get_subclass(node_name)
    edge = getattr(node, edge_name)
    return models.Node.get_subclass_named(edge.target_class.__dst_class__).__name__


def get_parent_label(models, node_name, edge_name):
    node = models.Node.get_subclass(node_name)
    edge = getattr(node, edge_name)
    return models.Node.get_subclass_named(edge.target_class.__dst_class__).get_label()


def get_node_label(models, node_name):
    node = models.Node.get_subclass_named(node_name)
    return node.get_label()


def get_node_table_name(models, node_name):
    node = models.Node.get_subclass(node_name)
    return node.__tablename__


def get_properties_types(models, node_name):
    node = models.Node.get_subclass(node_name)
    return node.__pg_properties__


def object_to_string(obj):
    # s = ','.join('{}: {}'.format(k, obj.__getattr__(k)) for k in obj.__dict__)
    return '<{}>'.format(obj.__dict__)


def select_widest_types(types):
    for k, v in types.items():
        types[k] = select_widest_type(v)
    return types


def select_widest_type(types):
    if str in types:
        return str
    elif float in types:
        return float
    elif long in types:
        return long
    elif int in types:
        return int
    else:
        return str


def generate_mapping(doc_name, field_types):
    """
    :param doc_name: name of the Elasticsearch document to create mapping for
    :param field_types: dictionary of field and their types
    :return: JSON with proper mapping to be used in Elasticsearch
    """
    es_type = {
        str: 'keyword',
        float: 'float',
        long: 'long',
        int: 'integer'
    }

    properties = {k: {'type': es_type[v]} for k, v in field_types.items()}

    # explicitly mapping 'node_id'
    properties['node_id'] = {'type': 'keyword'}

    mapping = {'mappings': {
        doc_name: {'properties': properties}
    }}
    return mapping


def get_attribute_from_path(models, root, path):
    if path != '':
        splitted_path = path.split('.')
    else:
        splitted_path = []

    for i in splitted_path:
        root, node = get_edge_table(models, root, i)
    return root


def get_multiplicity(dictionary, root, parent):
    schema = dictionary.schema

    links = schema[parent]['links']

    for link in links:
        if 'subgroup' in link:
            for l in link['subgroup']:
                if l['target_type'] == root:
                    return l['multiplicity']
        else:
            if link['target_type'] == root:
                return link['multiplicity']

    return
