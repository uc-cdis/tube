from tube.utils.db import get_db_cursor
from tube.utils.dd import object_to_string, get_all_edges_table, get_node_label, get_all_children_of_node, \
    get_edge_table, get_parent_name, get_node_table_name, get_parent_label, get_node_category
from tube.utils.general import PROJECT_CODE
from ..base.parser import Parser as BaseParser
from ..injection.nodes.collecting_node import CollectingNode, RootNode, LeafNode


class Path(object):
    def __init__(self, roots, path_name, src):
        self.roots = roots
        self.src = src
        self.path = Path.create_path(path_name)

    @classmethod
    def create_path(cls, s_path):
        return tuple(s_path.split('.'))

    def __key__(self):
        return (self.src,) + self.path

    def __hash__(self):
        return hash(self.__key__())

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__key__())

    def __eq__(self, other):
        return self.__key__() == other.__key__()


class NodePath(object):
    def __init__(self, class_name, upper_path):
        self.class_name = class_name
        self.upper_path = upper_path

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return self.__str__()


class Parser(BaseParser):
    def __init__(self, mapping, model, dictionary):
        super(Parser, self).__init__(mapping, model)
        self.dictionary = dictionary
        self.props = self.create_props_from_json(self.doc_type, self.mapping['props'],
                                                 node_label=self.get_first_node_label_with_category())
        self.leaves = set([])
        self.collectors = []
        self.roots = []
        self.generated_edges = self.get_edges_having_data()
        self.get_collecting_nodes()

    def get_first_node_label_with_category(self):
        selected_category = self.mapping.get('category', 'data_file')
        leaves_name = [k for (k, v) in list(self.dictionary.schema.items())
                       if v.get('category', None) == selected_category]
        if len(leaves_name) > 0:
            return leaves_name[0]
        return None

    def get_edges_having_data(self):
        tb_names = get_all_edges_table(self.model)
        query = ','.join('(select count(*) from {tb_name}) as {tb_name}'
                         .format(tb_name=tb_name) for tb_name in tb_names)
        query_statement = 'select {query};'.format(query=query)
        with get_db_cursor("db") as cur:
            cur.execute(query_statement)
            count_values = cur.fetchone()

        tb_names_with_data = []
        for i in range(len(tb_names)):
            if count_values[i] > 0:
                tb_names_with_data.append(tb_names[i])
        return tb_names_with_data

    def get_props_for_nodes(self):
        prop_nodes = {}
        for (k, v) in self.mapping['injecting_props'].items():
            if k == 'project' and 'project_code' not in v.get('props').values():
                v.get('props').append({'name': PROJECT_CODE, 'src': 'code'})
            prop_nodes[k] = CollectingNode(k, get_node_table_name(self.model, k),
                                           props=self.create_props_from_json(self.doc_type,
                                                                             v.get('props'),
                                                                             node_label=k))
        if 'project' not in prop_nodes.keys():
            prop_nodes['project'] = CollectingNode('project',
                                                   get_node_table_name(self.model, 'project'),
                                                   props=self.create_props_from_json(self.doc_type,
                                                                                     [{'name': PROJECT_CODE, 'src': 'code'}],
                                                                                     node_label='project'))
        return prop_nodes

    def get_collecting_nodes(self):
        def selected_category_comparer(dictionary, x):
            return get_node_category(dictionary, x) == selected_category

        selected_category = self.mapping.get('category', 'data_file')
        flat_paths = self.create_collecting_paths_from_root(
                    'program', lambda x: selected_category_comparer(self.dictionary, x)
                )
        leaves = set([p.src for p in flat_paths])
        for l in leaves:
            self.leaves.add(LeafNode(l, get_node_table_name(self.model, l)))

        if 'injecting_props' in self.mapping:
            nodes_with_props = self.get_props_for_nodes()
            self.collectors, self.roots = self.create_tree_from_generated_edges(flat_paths, nodes_with_props)

        self.update_level()
        self.collectors.sort()

    def update_level(self):
        """
        Update the level of nodes in the parsing tree
        :return:
        """
        level = 1
        assigned_levels = set([])
        just_assigned = set([])
        for root in self.roots:
            for child in root.children:
                if child in just_assigned:
                    continue
                child.level = level
                if len(child.children) == 0:
                    continue
                just_assigned.add(child)
        assigned_levels = assigned_levels.union(just_assigned)

        level += 1
        leaves = [c for c in self.collectors if len(c.children) == 0]
        len_non_leaves = len(self.collectors) - len(leaves)
        while len(assigned_levels) <= len_non_leaves and len(just_assigned) > 0:
            new_assigned = set([])
            for collector in just_assigned:
                for child in collector.children:
                    if child in assigned_levels:
                        continue
                    child.level = level
                    if len(child.children) == 0:
                        continue
                    new_assigned.add(child)
            just_assigned = new_assigned
            assigned_levels = assigned_levels.union(new_assigned)
            level += 1

    def add_root_node(self, child, roots, segment):
        root_name = get_node_label(self.model, get_parent_name(self.model, child.name, segment))
        _, edge_up_tbl = get_edge_table(self.model, child.name, segment)
        root_tbl_name = get_node_table_name(self.model, get_parent_label(self.model, child.name, segment))
        top_node = roots[root_name] if root_name in roots \
            else RootNode(root_name, root_tbl_name,
                          self.create_props_from_json(self.doc_type,
                                                      [{'name': 'program_name', 'src': 'name'}],
                                                      node_label=root_name))
        child.add_parent(top_node.name, edge_up_tbl)
        top_node.add_child(child)

        roots[root_name] = top_node

    def add_collecting_node(self, child, collectors, fst):
        parent_name = get_node_label(self.model, get_parent_name(self.model, child.name, fst))
        _, edge_up_tbl = get_edge_table(self.model, child.name, fst)
        tbl_name = get_node_table_name(self.model, get_parent_label(self.model, child.name, fst))
        collecting_node = collectors[parent_name] if parent_name in collectors \
            else CollectingNode(parent_name, tbl_name)
        collecting_node.add_child(child)
        child.add_parent(collecting_node.name, edge_up_tbl)
        collectors[parent_name] = collecting_node
        return collecting_node

    def create_tree_from_generated_edges(self, flat_paths, nodes_with_props):
        collectors = nodes_with_props
        roots = {}
        checking_set = set(self.generated_edges)
        for p in flat_paths:
            segments = list(p.path)
            _, edge_up_tbl = get_edge_table(self.model, p.src, segments[0])
            if edge_up_tbl not in checking_set:
                continue
            if p.src not in collectors:
                tbl_name = get_node_table_name(self.model, p.src)
                collectors[p.src] = CollectingNode(p.src, tbl_name)
            child = collectors[p.src]
            if len(segments) > 1:
                for fst in segments[0:len(segments)-1]:
                    _, edge_up_tbl = get_edge_table(self.model, p.src, segments[0])
                    if edge_up_tbl not in checking_set:
                        break
                    child = self.add_collecting_node(child, collectors, fst)
            self.add_root_node(child, roots, segments[-1])
        return list(collectors.values()), list(roots.values())

    def initialize_queue(self, label):
        name = self.model.Node.get_subclass(label).__name__
        processing_queue = []
        children = get_all_children_of_node(self.model, name)
        for child in children:
            processing_queue.append(NodePath(child.__src_class__, child.__src_dst_assoc__))
        return processing_queue

    def create_collecting_paths_from_root(self, label, selector):
        flat_paths = set()
        processing_queue = self.initialize_queue(label)
        i: int = 0
        while i < len(processing_queue):
            current_node = processing_queue[i]
            current_label = get_node_label(self.model, current_node.class_name)
            if selector(current_label):
                path = Path([label], current_node.upper_path, current_label)
                flat_paths.add(path)
            children = get_all_children_of_node(self.model, current_node.class_name)
            for child in children:
                processing_queue.append(
                    NodePath(child.__src_class__, '.'.join([child.__src_dst_assoc__, current_node.upper_path])))
            i += 1
        return flat_paths
