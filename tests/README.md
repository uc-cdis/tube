# Tube tests

The tests in directory `standalone_tests` can be run with pytest after installing the project, without additional setup.

The tests in directory `integrated_tests` require Spark and ElasticSearch to be running. See [this doc](/docs/run_tube_tests_locally.md) on how to run these tests locally.

The tests in directory `dataframe_tests` require Spark to be running.

## Dataframe tests

The dataframe tests are used to test Tube function by function. These tests require dataframes to be produced in every transformation step. A Spark cluster must be running, since the tests submit the ETL mapping and the dataframes to the Spark cluster, where the transformation happens.

Each test case should have an `input_df` and an `output_df`. When testing, simply compare and ensure that with the given `input_df`, we get the expected `output_df`.

Use `print(df.show(truncate=False))` to view the contents of a dataframe.

### How to generate test dataframes

We need to submit the original test data file to Sheepdog in a QA environment or a Gen3 instance running in local. Make sure to use the approopriate data dictionary, for example use the MIDRC dictionary to run tests marked as `schema_midrc`.

**Note:** all existing Sheepdog data must be cleared from the database before the test data is submitted.

When running the ETL, use the "PreTest" running mode (see modes [here](https://github.com/uc-cdis/tube/blob/cac298e/tube/enums.py#L1-L4) for reference) to export the intermediate result of each step to a Parquet file and store it in Hadoop HDFS:
```
python run_etl -c PreTest
```
