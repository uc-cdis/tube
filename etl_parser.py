import yaml
from pprint import pprint
from collections import defaultdict

path = '/tmp/ETLMap.yaml'
# open the yaml file and load it as dictionary
with open(path) as yamlfile:
    try:
        #print(yaml.safe_load(yamlfile))
        data = dict(yaml.safe_load(yamlfile))
    except yaml.YAMLError as exc:
        print(exc)

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

print(root_dict)
print(flatten_dict)
