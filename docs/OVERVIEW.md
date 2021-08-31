# Overview of ETL process
## Background
Metadata of data file submitted to commons are stored in PostgreSQL. Query data from PostgreSQL with multiple join is painful and inefficient. So, we use ElasticSearch as a place to store materialized dataset. Extract-transform-load (ETL) is a process that creates the materialized data from PostgreSQL and store them in ElasticSearch.

### Extract
There are multiple ways to extract data from an existing SQL database such as:
- **Extract** original data from data source to get necessary data and storing in data transformer's memory by SQL query statements, then **transform** to expected format and finally **load** output data into new data source as materialized data.
- **Extract** SQL data from the original data source by physically (dumping all unnecessary data) to an external data source. Then, process to extract necessary information from that external one with another technology (usually a parallel framework) in the transformation stage.

### Transform
In the data transformation stage, a series of rules or functions are applied to the extracted data in order to shape it in an expected format for loading into the end target. In our case, we want to create indexing data so that the front-end can perform query in the most efficient way.

### Load
There are two loading stage, one is loading data from traditional database to the place that can be easily processed by the **transformer**, another one is loading from **transformer** to the target data source.


## Data Transformation
Choosing framework to do data transformation (a.k.a data pipeline) is the most important thing in ETL, because every data pipeline requires a specific format of input and output data.
In specific to our use-case, [Apache Spark](https://spark.apache.org/) is one of the most advanced data processing technology, because its distributed architecture allows:
 1. processing data in parallel simply inside the horizontally scalable memory.
 2. iteratively processing data in multiple steps without reloading from data storage (disk). 
 3. streaming and integrating incremental data to an existing data source.

Hence, we choose Spark as a data transformer for a fast and scalable data processing. 

As discussed previously, there are multiple ways to extract data from database and load to Spark. One is directly generate and execute in parallel multiple SQL queries and load it to Spark's memory, another one is dumping the whole dataset to intermediate data storage like HDFS and then load text data stored in HDFS into Spark in parallel.  

Learning all the options that one of our collabators OICR tried (posted [here](https://softeng.oicr.on.ca/grant_guo/2017/08/14/spark/)). We decided to go with similar strategy - dump postgres to HDFS and load HDFS to rdd/SPARK.
We decided to use [SQOOP](https://github.com/apache/sqoop) to dump the postgres database to HDFS. In order to dump PostgreSQL database, SQOOP calls [CopyManager](https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html).

Finally, we decided to use python instead of scala because cdis dev teams are much more comfortable with python programming. And since all the computation will be done in spark, we won't do any manipulation on the python level, the performance won't be a huge difference.
