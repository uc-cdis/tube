# self.dictionary = dictionary
# mapping_type = mapping['type']
# self.root = mapping['root']


# key_list = ['name', 'doc_type', 'type', 'root', 'props', 'flatten_props', 'aggregated_props']
# props_attr_list = ['name', 'src', 'path', 'fn']
# collector_list = ['name', 'doc_type', 'type', 'root', 'category', 'props', 'injecting_props']

def validate(mapping, model):
    key_list = ['name', 'doc_type', 'type', 'root', 'props', 'flatten_props', 'aggregated_props']
    props_attr_list = ['name', 'src', 'path', 'fn', 'value_mappings', 'props', 'sorted_by']
    collector_list = ['name', 'doc_type', 'type', 'root', 'category', 'props', 'injecting_props']
    mapping_type = mapping['type']
    list_of_nodes = []
    all_classes = model.Node.get_subclasses()
    nodes_with_props = {}
    for n in all_classes:
        present_node = n._pg_edges
        present_props = n.__pg_properties__
        present_node_props = [k for k in present_props.keys()]
        for v in present_node.values():
            respo = v['backref']
            nodes_with_props.update({respo: present_node_props})
            list_of_nodes.append(respo)

    list_of_nodes = list(set(list_of_nodes))

    if mapping_type == 'aggregator':
        for key, value in mapping.items():
            if key in key_list:
                check_none = mapping.get(key)
                if check_none == '':
                    print "Error in root properties values"
                elif key == 'props' and value != '_ALL':
                    for n in value:
                        if n['name'] not in nodes_with_props['subjects']:
                            print "Something wrong with root node properties, not find in dictionary"

                elif key == 'flatten_props':
                    for n in value:
                        if not all(elem in props_attr_list for elem in n.keys()):
                            print "Error-Node name wrong"
                        if n['path'] not in list_of_nodes:
                            print "Path doesn't exist in dictionary"
                        elif n.values() == '':
                            print "Error in Flatten properties"
                        if n['props'] != '_ALL':
                            for l in n['props']:
                                if l['name'] == '':
                                    print "Error in Flatten properties- Null Values"
                                elif l['name'] not in nodes_with_props[n['path']]:
                                    print "Something wrong with Flatten properties, not find in dictionary"

                elif key == 'aggregated_props' and value != '_ALL':
                    for n in value:
                        if not all(elem in props_attr_list for elem in n.keys()):
                            print "Error-Node name wrong"
                        elif n.values() == '':
                            print "Error in properties value- name, src, fn etc - Null values"
                        if 'fn' in n.keys():
                            if n['fn'] not in ['set', 'count', 'list', 'sum', 'min']:
                                print "Error in Function under mapping file - " + n['fn']
                        if 'path' in n.keys():
                            split_node = n['path'].split('.')
                            if '_ANY' in split_node:
                                split_node.remove("_ANY")
                                for s in split_node:
                                    if s not in list_of_nodes:
                                        print "Aggregated props path not find in dictionary"
                            else:
                                for s in split_node:
                                    if s not in list_of_nodes:
                                        print "Aggregated props path not find under dictionary"

                        if 'src' in n.keys():
                            split_node = n['path'].split('.')
                            if n['src'] not in nodes_with_props[split_node[-1]]:
                                print "src not find under dictionary"

                elif key == 'joining_props':
                    join_list = ['index', 'join_on', 'props']
                    # print value
                    for n in value:
                        if not all(elem in join_list for elem in n.keys()):
                            print "Error-Node name wrong"
                        elif n.values() == '':
                            print "Error in properties value- index, join_on, props etc - Null values"

                        for i in n['props']:
                            if not all(elem in props_attr_list for elem in i.keys()):
                                print "Error-Node name wrong"
                            if 'fn' in i.keys():
                                if i['fn'] not in ['set', 'count', 'list', 'sum', 'min']:
                                    print "Error in Function under mapping file - " + i['fn']

            else:
                print "Root Node attributes are missing"

    if mapping_type == 'collector':
        for key, value in mapping.items():
            if key in collector_list:
                collector_key_value = mapping.get(key)
                if collector_key_value == '':
                    print "Error in Collector Properties"
                elif key == 'props' and collector_key_value != '_ALL':
                    for n in value:
                        if n.values() == '':
                            print "Error in properties of collector -"

                elif key == 'injecting_props':
                    for k, v in value.items():
                        inject_props = v.get('props')
                        for a in inject_props:
                            if not all(elem in props_attr_list for elem in a.keys()):
                                print "Error in collector properties attributes - 'props'"
                            elif a.values() == '':
                                # print a.values()
                                print "Error in properties of collector - Blank values"
                            else:
                                "Error in Collector Attributes, some attributes are missing"
            else:
                print "Error in Collector Attributes, some attributes are missing"

    return mapping
