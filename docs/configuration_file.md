# How to configure the Gen3 Tube ETL

- [Background](#background)
- [Configuration file](#configuration-file)
  - [The aggregation ETL ("aggregator" mapping)](#the-aggregation-etl-aggregator-mapping)
    - [flatten_props](#flatten_props)
    - [parent_props](#parent_props)
    - [aggregated_props](#aggregated_props)
    - [joining_props](#joining_props)
  - [The injecting ETL ("collector" mapping)](#the-injecting-etl-collector-mapping)
    - [injecting_props](#injecting_props)

## Background

We need a way to specify how we want to materialize data from the "source of truth" database to an indexing data source.

Given a schema with multiple "one-to-many" or "many-to-many" relationships, there are several ways of joining data from different tables.
1. Storing complete children (relationally) tables inside the parent (relationally) table so that we can freely do whatever we want later.
2. Pre-computing all necessary fields of an index.
3. Adding extra/redundant keys from the parent node to the children nodes to reduce the time needed to join nodes.

We choose the later two approaches to implement in Tube. They are implemented as two mapping syntaxes:
1. The "aggregation" syntax (mapping type: `aggregator`) allows to pre-compute and integrate data from multiple nodes in the original dataset to an individual one in the target dataset.
2. The "injection" syntax (mapping type: `collector`) allows to embed some fields in a high level node to lower level nodes to reduce the time needed to join.

## Configuration file

The Gen3 Tube ETL is configured through an `etlMapping.yaml` configuration file, which describes which tables and fields to ETL to ElasticSearch. It follows the syntax below.

```
mappings:                         # list of mappings - one mapping per index to create
- name: my-data-commons_subject # ElasticSearch index name
  doc_type: subject             # document type - used to query the index. The Guppy config must match
  type: aggregator              # mapping type ("aggregator" or "collector")
  root: subject                 # if "aggregator": root node in the input database
  < properties mapping >
- name: my-data-commons_file
  doc_type: file
  type: collector
  category: data_file           # if "collector": node category to collect properties from. Default: "data_file"
  < properties mapping >
```

##### Properties
For all the indices, the properties (`props`) are a fundamental concept representing the all fields that are expected in the final ElasticSearch index. For every property, we must specify the `name` as the property's final name in the output index.

The fields below can be set for any type of property in the configuration file.

###### src (optional)
The original name of the property in the node, if different from `name`. Example:
```
props:
- name: participant_gender  # property name in the index
  src: gender               # property name in the database
```

###### value_mappings (optional)
A list of values which should be renamed for a specify property, along with the values they should be replaced with. Example:
```
props:
- name: gender
  value_mappings:
  - f: Female
  - m: Male
```

### The aggregation ETL ("aggregator" mapping)

**When to use it?** When creating an index from a single "root" node. Properties from parent and child nodes can be added, as long as there are direct or indirect links between the root node and the other nodes.

Example:
```
mappings:
- name: my-data-commons_subject # ElasticSearch index name
  doc_type: subject             # document type - used to query the index. The Guppy config must match
  type: aggregator
  root: subject                 # root node in the input database
  props:
  - [...]
  flatten_props:
  - [...]
  aggregated_props:
  - [...]
  parent_props:
  - [...]
  joining_props:
  - [...]
```

#### props
Properties from the root node. Example:
```
props:
- name: submitter_id
- name: project_id
```

#### flatten_props
Properties from other nodes which are "below" the root node in the graph data model (child nodes), if the added information does not exponentially increase the size of the final document.

It is straightforward to configure nodes which have a "one-to-one" relationship with the root node. Nodes that have a "many-to-*" relationship with the root node can be configured by specifying an aggregation function (`fn`) on the properties, or by using a `sorted_by` declaration.

Example:
```
flatten_props:
- path: demographics # the "backref" name of the link in the dictionary
  props:             # properties to include
  - name: gender
  - name: race
```

###### path
Specify the path from the root node, through the intermediate nodes, to the final node which contains the property to add to the index. Dots `.` can be used to add more than one node to the path.

###### sorted_by (optional)
In the case of a "many-to-*" relationship, multiple rows are returned by the join. We can select which row to add to the index by setting `sorted_by` to a property. Example:
```
flatten_props:
- path: subjects
  props:
  - name: submitter_id
  sorted_by: updated_datetime, desc
```

#### nested_props
Properties from the children nodes that can be nested into the parent node. THe nested properties are defined under an aggregator starting with keyword `nested_props` black.

Every `nested_props` block has following required fields `name, path, props`. In which, `name` is field in elasticsearch index that we want to be appeared. `path` is the path leading to the children nodes that we want to get the data out. `props` is the properties that we want to add into the nested structure.

In the example below. We can create under `subject` index a nested structure of `tumor_assessments`. The `tumor_assessments` can be linked to `subject` by a direct link or indirect link via `events`. We can have both in the same index (if necessary) by just naming it differently. In this example, we will get some fields of the intermediate node `events` and add into the nested structure.

```
mappings
  - name: pcdc
    doc_type: subject
    type: aggregator
    root: subject
    props:
      - name: subject_submitter_id
        src: submitter_id
      - name: project_id
      - name: age_at_enrollment
      - name: year_at_enrollment
    nested_props:
      - name: tumor_under_subject_directly
        path: tumor_assessments
        props:
          - name: tumor_classification
          - name: tumor_site
      - name: event_and_tumor_under_subject
        nested_props:
        - name: event_under
          path: events
          props:
            - name: time_when_it_happen
            - name: others_props
          nested_props:
            - name: tumor_under_event
              path: tumor_assessments
              props:
                - name: tumor_classification
                - name: tumor_site
```

In the following example, if we don't want to get any data field of the intermediate node `event`, we can specify the path as `events.tumor_assessments`

```
mappings
  - name: pcdc
    doc_type: subject
    type: aggregator
    root: subject
    props:
      - name: subject_submitter_id
        src: submitter_id
      - name: honest_broker_subject_id
      - name: project_id
      - name: age_at_enrollment
      - name: year_at_enrollment
    nested_props:
      - name: tumor_under_subject_directly
        path: tumor_assessments
        props:
          - name: tumor_classification
          - name: tumor_site
      - name: event_and_tumor_under_subject
        path: events.tumor_assessments
        props:
          - name: tumor_classification
          - name: tumor_site
```


#### parent_props
Properties from other nodes which are "above" the root node in the graph data model (parent nodes). Example:
```
parent_props:
- path: subjects[name]
- path: studies[study_objective,study_submitter_id:submitter_id].projects[project_name:name]
```

###### path
Specify the path from the root node, through the intermediate nodes, to the final node which contains the property to add to the index.

In the example above, the root node has a "subject" parent node (the link "backref" is "subjects"), as well as a "study" parent node which itself has a "project" parent node. The final index will contain the `name` of the parent subjects, the `study_objective` and the `submitter_id` (renamed `study_submitter_id` in the final index) of the parent studies, and the `name` (renamed `project_name` in the final index) of the parent project.

#### aggregated_props
Used to get aggregate statistics of nested nodes. Pre-computed properties that require multiple joins. A `path` is required for every property. Example:
```
aggregated_props:    # used to get aggregate statistics of nested nodes
- name: sample_count # gets the count of this node name
  path: samples      # path to this node from the root
  fn: count
- name: treatment_status
  src: treatment_status
  path: follow_ups.treatments
  fn: set
```

###### path
Specify the path from the root node, through the intermediate nodes, to the final node which contains the property to add to the index. Dots `.` can be used to add more than one node to the path.

###### fn (optional)
The aggregation function to be executed with the property. Tube supports 6 aggregation functions:
  - `count`
  - `max`
  - `min`
  - `sum`
  - `list`
  - `set`

#### joining_props
Used to join two indices, for example, to get all the files associated with a cohort of cases. Example:
```
joining_props:
- index: file      # the index to join on
  join_on: case_id # the identifier to join on (it should be in both indices)
  props:
  - name: data_format
    src: data_format
    fn: set
  - name: data_type
    src: data_type
    fn: set
```

###### fn (optional)
See [fn](#fn-optional).

### The injecting ETL ("collector" mapping)

The "injection" approach allows us to redundantly embed the parent node's ID into its grandchildren nodes. This reduces the time needed to perform join operation between nodes.

**When to use it?** When creating an index from multiple nodes which share some properties. Use the `category` field to choose a node category; all the nodes in this category will be "collected".

Example:
```
mappings:
- name: my-data-commons_file # ElasticSearch index name
  doc_type: file             # document type - used to query the index. The Guppy config must match
  type: collector
  category: data_file        # node category to collect properties from. Default: "data_file"
  props:
  - [...]
  injecting_props:
  - [...]
```

#### props
Properties from the root node. Example:
```
props:
- name: submitter_id
- name: project_id
- name: source_node   # special built-in prop - see note below
```

###### source_node

This special built-in property can be used in `collector` mappings to include the name of node from which the entity comes from. For example, if a data file is from the `reference_file` node in the input database, the value of `source_node` will be `"reference_file"`.

This is useful for working with data files because in many dictionaries, data files can be located in one of several nodes, and it can be helpful to know where each data file comes from. For example, the PFB export of data files in the Gen3 data-portal relies on `source_node` in order to tell the `pelican-export` job where in the dictionary to search for data files.

#### injecting_props

All `injecting_props` are grouped by the node containing the properties. In the example below, that node is `subject`.

```

injecting_props:
  subject:
    props:
    - name: subject_id
      src: id
    - name: project_id
```
