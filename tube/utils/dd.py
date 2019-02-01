from dictionaryutils import DataDictionary, dictionary


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
    """
    Return the a table name indicated by node's name and edge name. Work as bidirectional link
    :param models: considering model
    :param node_name: known node name
    :param edge_name: link from/to node_name
    :return:
    """
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


def get_attribute_from_path(models, root, path):
    if path != '':
        splitted_path = path.split('.')
    else:
        splitted_path = []

    for i in splitted_path:
        root, node = get_edge_table(models, root, i)
    return root


def get_multiplicity(dictionary, parent, child):
    schema = dictionary.schema

    links = schema[child]['links']

    for link in links:
        if 'subgroup' in link:
            for l in link['subgroup']:
                if l['target_type'] == parent:
                    return l['multiplicity']
        else:
            if link['target_type'] == parent:
                return link['multiplicity']

    return
