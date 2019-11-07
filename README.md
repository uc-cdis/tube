# Gen3 ETL - a process from Postgresql to ES

[![Build Status](https://travis-ci.com/uc-cdis/tube.svg?branch=feat/test-travis)](https://travis-ci.com/uc-cdis/tube)

Providing a quick response for every data query is challenging, since we need to balance the drawback of the data storage space and the performance of the query. Given a database schema represented in the figure, query the whole data from all data tables requires multiple joins to connect data to each other.

![Data schema](docs/dictionary.png)

SQL databases provide us an optimal and standard way to store data. However, we must pay the cost to process or retrieve data for that optimization on the saving space. Given a database with schema as in the figure above, in order to gather all information related to `Subject` in descendant table, we need perform sixteen joins (one per link). With big data, it is an expensive task.

NoSQL and documented database offer a way to circumvent the cost by duplicating data or materializing necessary value for the frequent requests. Normally, when data being received by the system, they are stored in the source-of-truth database and being streamed to the secondary documented database via the Extract-Transform-Load (ETL) process.  

Gen3 ETL is designed to translate data from a graph data model stored in Postgresql database to indexed documents in ElasticSearch (ES) which supports the efficient way to query data from the front-end. As all other general ETL process, the purpose of Gen3 ETL is to create indexed document to reduce the responding time of every request to query data.

Refer to [Overview](docs/OVERVIEW.md) for more informaiton about general ETL process. In the section below, we specifically focus on Gen3 ETL's transformer.

## Mapping
We need a way to specify how we want to materialize data from `source-of-truth` database to an indexing data source.

Given a one-to-many relationship, literally, there are several ways of joining data from different table together.
1. Storing complete complete table inside another table so t.
2. Pre-computing all necessary fields of a index 

### Mapping file
Every ETL process is defined by a translation from the original dataset to the expected one. *Gen3-ETL* provides a neutral way with which you can:
 1. `aggregate, collect` data from mulitple nodes in original dataset to an individual one in the target dataset.
 2. `embed` some fields in high level node to lower level nodes or `extract` some particular fields from any specific node.

The format of mapping file:
```
mappings:
  - name: NAME_OF_THE_INDEX_in_ES
    doc_type: DOCTYPE_OF_THE_INDEX_in_ES
    type: {aggregator or collector}
    root: ROOT_NODE_OF_THE_INDEX
    props:
      #  Properties of the root node that are put in the index
      - name: submitter_id
      - name: project_id
    flatten_props:
      # list of properties together with paths to the nodes 
      # have 1-to-1 relationship with the root node
      - path: demographics
        props:
          # properties from node specified by the path
          - name: gender_ES
            # name of field in the ES index
            src: gender
            # name of field in the original database
            value_mappings:
            # mapping rule from value in SQL database 
            # to the value in ES
              - female: F
              - male: M
    aggregated_props:
      # the name, the path to the node and the aggretating func
      # support only count func now
      - name: _summary_lab_results_on_sample_count
        path: samples.summary_lab_results
        fn: count

```
