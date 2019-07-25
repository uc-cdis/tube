# Gen3 ETL - a process from Postgresql to ES

[![Build Status](https://travis-ci.com/uc-cdis/tube.svg?branch=feat/test-travis)](https://travis-ci.com/uc-cdis/tube)
## Introduction to ETL
### Background
Metadata of data file submitted to commons are stored in Postgresql. Query data from Postgresql with multiple join is painful and inefficient. So, we use ElasticSearch as a place to store materialized dataset. Extract-transform-load (ETL) is a process that creates the materialized data from Postgresql and store them in ElasticSearch.

#### Extract
There are multiple ways to extract data from an existing SQL database such as:
- **Extract** original data from data source to get necessary data and storing in data transformer's memory by SQL query statements, then **transform** to expected format and finally **load** output data into new data source as materialized data.
- **Extract** SQL data from the original data source by physically (dumping all unnecessary data) to an external data source. Then, process to extract necessary information from that external one with another technology (usually a parallel framework) in the transformation stage.

#### Transform
In the data transformation stage, a series of rules or functions are applied to the extracted data in order to shape it in an expected format for loading into the end target. In our case, we want to create indexing data so that the front-end can perform query in the most efficient way.

#### Load
There are two loading stage, one is loading data from traditional database to the place that can be easily processed by the **transformer**, another one is loading from **transformer** to the target data source.

### Gen3 ETL
Gen3 ETL is designed to translate data from a graph data model stored in Postgresql database to flatten indices in ElasticSearch (ES) which supports the efficient way to query data from the front-end.
#### Transformer
Interestingly, choosing transformer is the most important thing in ETL, because transformer requires a specific format of input and output data.
Specifically to our use-case, Spark becomes one of the most advanced data processing technology, because its distributed architecture allows:
 1. processing data in parallel simply inside the horizontally scalable memory.
 2. iteratively processing data in multiple steps without reloading from data storage (disk).
 3. streaming and integrating incremental data to an existing data source.

Hence, we choose Spark as a data transformer for a fast and scalable data processing.

As discussed previously, there are multiple ways to extract data from database and load to Spark. One is directly generate and execute in parallel multiple SQL queries and load it to Spark's memory, another one is dumping the whole dataset to intermediate data storage like HDFS and then load text data stored in HDFS into Spark in parallel.

Learning all the options that one of our collabators OICR tried (posted [here](https://softeng.oicr.on.ca/grant_guo/2017/08/14/spark/) ). We decided to go with similar strategy - dump postgres to HDFS and load HDFS to rdd/SPARK.
We decided to use [SQOOP](https://github.com/apache/sqoop) to dump the postgres database to HDFS. In order to dump postgresql database, SQOOP calls [CopyManager](https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html).

<!-- We decided to use spark for running the ETL process, because:
1. It was used in our previous projects so we have familiarity with the framework and a certain level of confidence that it will work.
2. It scales very well with the data. We don't need to change any code when the data scale changes.
3. Good python API that's low learning curve for cdis dev teams. -->

Finally, we decided to use python instead of scala because cdis dev teams are much more comfortable with python programming. And since all the computation will be done in spark, we won't do any manipulation on the python level, the performance won't be a huge difference.

#### Mapping file
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