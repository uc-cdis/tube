# Overview of ETL process
## Background
Metadata of data file submitted to commons are stored in Postgresql. Query data from Postgresql with multiple join is painful and inefficient. So, we use ElasticSearch as a place to store materialized dataset. Extract-transform-load (ETL) is a process that creates the materialized data from Postgresql and store them in ElasticSearch.

### Extract
There are multiple ways to extract data from an existing SQL database such as:
- **Extract** original data from data source to get necessary data and storing in data transformer's memory by SQL query statements, then **transform** to expected format and finally **load** output data into new data source as materialized data.
- **Extract** SQL data from the original data source by physically (dumping all unnecessary data) to an external data source. Then, process to extract necessary information from that external one with another technology (usually a parallel framework) in the transformation stage.

### Transform
In the data transformation stage, a series of rules or functions are applied to the extracted data in order to shape it in an expected format for loading into the end target. In our case, we want to create indexing data so that the front-end can perform query in the most efficient way.

### Load
There are two loading stage, one is loading data from traditional database to the place that can be easily processed by the **transformer**, another one is loading from **transformer** to the target data source.
