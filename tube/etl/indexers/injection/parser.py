from tube.utils.dd import get_edge_table, get_node_table_name, get_node_label, get_parent_name, get_parent_label, \
    object_to_string, get_all_children_of_node, get_node_category
from tube.etl.indexers.injection.nodes.collecting_node import CollectingNode, RootNode, LeafNode
from ..base.parser import Parser as BaseParser


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
        self.get_collecting_nodes()

    def get_first_node_label_with_category(self):
        # if len(self.mapping['injecting_props'].items()) == 0:
        #     return None
        selected_category = self.mapping.get('category', 'data_file')
        leaves_name = [k for (k, v) in list(self.dictionary.schema.items())
                       if v.get('category', None) == selected_category]
        if len(leaves_name) > 0:
            return leaves_name[0]
        return None

    def get_orphan_paths(self, selected_category, leaves):
        leaves_name = [k for (k, v) in list(self.dictionary.schema.items())
                       if v.get('category', None) == selected_category]
        orphan_leaves = set([])
        for name in leaves_name:
            self.leaves.add(LeafNode(name, get_node_table_name(self.model, name)))
            if name not in leaves:
                orphan_leaves.add(name)

        if len(orphan_leaves) > 0:
            return self.get_shortest_path_from_root(['program', 'project'], orphan_leaves)
        return set([])

    def get_collecting_nodes(self):
        def selected_category_comparer(dictionary, x):
            return get_node_category(dictionary, x) == selected_category

        selected_category = self.mapping.get('category', 'data_file')
        flat_paths = set([])
        if 'injecting_props' in self.mapping:
            for k, v in list(self.mapping['injecting_props'].items()):
                flat_paths |= self.create_collecting_paths_from_root(k, lambda x: selected_category_comparer(
                                                                         self.dictionary, x)
                                                                     )
        leaves = [p.src for p in flat_paths]

        if 'injecting_props' in self.mapping:
            self.collectors, self.roots = self.construct_reversed_collection_tree(flat_paths)

        flat_paths = self.get_orphan_paths(selected_category, leaves)
        orphan_collectors, auth_root = self.construct_auth_path_tree(flat_paths)
        self.collectors.extend(orphan_collectors)
        self.roots.append(auth_root)

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
                if len(child.children) == 0 or child in just_assigned:
                    continue
                child.level = level
                just_assigned.add(child)
        assigned_levels = assigned_levels.union(just_assigned)

        level += 1
        leaves = [c for c in self.collectors if len(c.children) == 0]
        len_non_leaves = len(self.collectors) - len(leaves)
        while len(assigned_levels) != len_non_leaves:
            new_assigned = set([])
            for collector in just_assigned:
                for child in collector.children:
                    # TODO: PXP-5067 investigate int-string comparison in node lvls
                    # (Why were we assigning edge tbl names to leaf node lvls?)

                    # For now, commenting this out...
                    #if len(child.children) == 0 or child in assigned_levels:
                    #    continue
                    #child.level = level
                    #new_assigned.add(child)

                    # ...and proposing this fix:
                    if child in assigned_levels:
                        continue
                    child.level = level
                    if len(child.children) != 0:
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
                                                      self.mapping['injecting_props'][root_name]['props'],
                                                      node_label=root_name))
        child.add_parent(top_node.name, edge_up_tbl)
        top_node.add_child(child)

        roots[root_name] = top_node

    def add_collecting_node(self, child, collectors, fst):
        parent_name = get_node_label(self.model, get_parent_name(self.model, child.name, fst))
        _, edge_up_tbl = get_edge_table(self.model, child.name, fst)
        collecting_node = collectors[parent_name] if parent_name in collectors \
            else CollectingNode(parent_name)
        collecting_node.add_child(child)
        child.add_parent(collecting_node.name, edge_up_tbl)
        collectors[parent_name] = collecting_node
        return collecting_node

    def construct_reversed_collection_tree(self, flat_paths):
        collectors = {}
        roots = {}
        for p in flat_paths:
            segments = list(p.path)
            _, edge_up_tbl = get_edge_table(self.model, p.src, segments[0])
            if p.src not in collectors:
                collectors[p.src] = CollectingNode(p.src, edge_up_tbl)
            child = collectors[p.src]
            if len(segments) > 1:
                for fst in segments[0:len(segments)-1]:
                    child = self.add_collecting_node(child, collectors, fst)
            self.add_root_node(child, roots, segments[-1])
        return list(collectors.values()), list(roots.values())

    def create_auth_path_root(self):
        program_table_name = get_node_table_name(self.model, 'program')
        project_table_name = get_node_table_name(self.model, 'project')
        _, edge_up_tbl = get_edge_table(self.model, 'project', 'programs')
        root_program = RootNode('auth_path_root', program_table_name,
                                self.create_props_from_json(self.doc_type,
                                                            [{'name': 'program_name', 'src': 'name'}],
                                                            node_label='program'))
        root_project = RootNode('project', project_table_name,
                                self.create_props_from_json(self.doc_type,
                                                            [{'name': 'project_code', 'src': 'code'}],
                                                            node_label='project'), edge_up_tbl)
        root_program.root_child = root_project

        return root_program

    def construct_auth_path_tree(self, flat_paths):
        collectors = {}
        root = self.create_auth_path_root()
        for p in flat_paths:
            segments = list(p.path)
            _, edge_up_tbl = get_edge_table(self.model, p.src, segments[0])
            if p.src not in collectors:
                collectors[p.src] = CollectingNode(p.src, edge_up_tbl)
            child = collectors[p.src]
            if len(segments) > 1:
                for node in segments[0:len(segments)-2]:
                    child = self.add_collecting_node(child, collectors, node)
                _, edge_up_tbl = get_edge_table(self.model, child.name, segments[-2])
            elif len(segments) == 1:
                _, edge_up_tbl = get_edge_table(self.model, child.name, segments[-1])
            root.add_child(child)
            child.add_parent('auth_path_root', edge_up_tbl)

        return list(collectors.values()), root

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
        i = 0
        while (i < len(processing_queue)):
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

    def get_shortest_path_from_root(self, roots, nodes):
        cloned_nodes = nodes.copy()
        flat_paths = set()
        processing_queue = self.initialize_queue(roots[0])
        i = 0
        while len(cloned_nodes) > 0 and i < len(processing_queue):
            current_node = processing_queue[i]
            current_label = get_node_label(self.model, current_node.class_name)
            if current_label in cloned_nodes:
                path = Path(roots, current_node.upper_path, current_label)
                flat_paths.add(path)
                cloned_nodes.remove(current_label)
            children = get_all_children_of_node(self.model, current_node.class_name)
            for child in children:
                processing_queue.append(
                    NodePath(child.__src_class__, '.'.join([child.__src_dst_assoc__, current_node.upper_path])))
            i += 1
        return flat_paths
