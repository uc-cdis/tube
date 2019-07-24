import yaml
from pprint import pprint
from collections import defaultdict

class Parser(object):
    
    def __init__(self):
        pass
        
    def parser(self):
        #with open('/tmp/ETLMapping.yaml') as yamlfile:
            #try:
                #print(yaml.safe_load(yamlfile))
                #data = dict(yaml.safe_load(yamlfile))
            #except yaml.YAMLError as exc:
                #print(exc)

        props_values = []
        root_dict = {}
        flatten_dict = {}

        for i in range(len(data['mappings'])):
            root_dict.update({data['mappings'][i].get("doc_type", ""): ""})
            root_props = data['mappings'][i].get("props", "")
            root_dict[list(root_dict)[i]] = [d['name'] for d in root_props]
            for i in range(len(one_data['flatten_props'])):
                flatten_dict.update({one_data['flatten_props'][i].get("path", ""): ""})
                root_props = one_data['flatten_props'][i].get("props", "")
                flatten_dict[list(flatten_dict)[i]] = [d['name'] for d in root_props]
            return flatten_dict
        return root_dict
            
p1 = Parser()
f = open('/tmp/ETLMap_any_all.yaml', "r")
data = dict(yaml.safe_load(f))
p1.parser()
