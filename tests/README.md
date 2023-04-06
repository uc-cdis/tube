# Tube tests

The tests in directory `standalone_tests` can be run with pytest after installing the project, without additional setup.

The tests in directory `integrated_tests` require Spark and ElasticSearch to be running.

The tests in directory `dataframe_tests` require Spark to be running. Those tests are used to test tube function by function. These tests need dataframes produced in every transformation step. These dataframe is created by running `python run_etl -c PreTest`. Each test case should have an `input_df` and `output_df`. When test simply compare and ensure that with given `input_df`, we have an expected `output_df`.
