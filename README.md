# Gen3 ETL - a process from Postgresql to ES

[![Build Status](https://travis-ci.com/uc-cdis/tube.svg?branch=feat/test-travis)](https://travis-ci.com/uc-cdis/tube)

Providing a short responding time for every data query is challenging, since we need to balance the drawback of the data storage space and the performance of the query. Given a database schema represented in the figure, query the whole data from all data tables requires multiple joins to connect data to each other.

![Data schema](docs/dictionary.png)

On one hand,  since the JOIN operation in SQL Database query is expensive while 

Gen3 Extract-Transform-Load (Gen3 ETL) is designed to translate data from a graph data model stored in Postgresql database to indexed documents in ElasticSearch (ES) which supports the efficient way to query data from the front-end. As all other general ETL process, the purpose of Gen3 ETL is to create indexed document to reduce the responding time of every request to query data.

Refer to [Overview](docs/OVERVIEW.md) for more informaiton about general ETL process. In the section below, we specifically focus on Gen3 ETL's transformer.
 
## Data Transformation
Choosing framework to do data transformation (a.k.a data pipeline) is the most important thing in ETL, because every data pipeline requires a specific format of input and output data.
In specific to our use-case, [Apache Spark](https://spark.apache.org/) is one of the most advanced data processing technology, because its distributed architecture allows:
 1. processing data in parallel simply inside the horizontally scalable memory.
 2. iteratively processing data in multiple steps without reloading from data storage (disk). 
 3. streaming and integrating incremental data to an existing data source.

Hence, we choose Spark as a data transformer for a fast and scalable data processing. 

As discussed previously, there are multiple ways to extract data from database and load to Spark. One is directly generate and execute in parallel multiple SQL queries and load it to Spark's memory, another one is dumping the whole dataset to intermediate data storage like HDFS and then load text data stored in HDFS into Spark in parallel.  

Learning all the options that one of our collabators OICR tried (posted [here](https://softeng.oicr.on.ca/grant_guo/2017/08/14/spark/) ). We decided to go with similar strategy - dump postgres to HDFS and load HDFS to rdd/SPARK.
We decided to use [SQOOP](https://github.com/apache/sqoop) to dump the postgres database to HDFS. In order to dump postgresql database, SQOOP calls [CopyManager](https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html).

Finally, we decided to use python instead of scala because cdis dev teams are much more comfortable with python programming. And since all the computation will be done in spark, we won't do any manipulation on the python level, the performance won't be a huge difference.

## Mapping
We need a way to specify the way that we want to transform data from `source-of-truth` database to an indexing data source.

Given a one-to-many relationship, literally, there are three ways of joining data from different table together.
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
