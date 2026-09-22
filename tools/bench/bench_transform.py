"""
Benchmark the tube transform stage against the dataframe-test fixtures.

Runs the translator pipeline (the part the Spark tuning affects) directly on
the parquet/text fixtures, so no Postgres, Sqoop, HDFS or Elasticsearch is
needed. Prints one JSON object per run to stdout.

Usage:
    python tools/bench/bench_transform.py --schema ibdgc --doc-type participant \
        --indexer aggregation --repeat 3
"""
import argparse
import json
import os
import resource
import time
from unittest.mock import patch

TEST_DATA_HOME = "./tests/dataframe_tests/test_data"


def build_translator(schema_name, mapping_name, indexer_type, sc):
    import tube.settings as config  # noqa: F401  (import for side effects)
    from tube.utils.dd import init_dictionary
    from tests.util import mock_dictionary_url, initialize_mappings
    from tube.etl.indexers.aggregation.new_translator import (
        Translator as AggregationTranslator,
    )
    from tube.etl.indexers.injection.new_translator import (
        Translator as InjectionTranslator,
    )

    p = patch(
        "dictionaryutils.load_schemas_from_url",
        return_value=mock_dictionary_url(schema_name),
    )
    p.start()
    dictionary, model = init_dictionary("not_used_url")
    mapping = initialize_mappings(schema_name, mapping_name)
    hdfs_path = os.path.join(TEST_DATA_HOME, schema_name, "graphs")

    if indexer_type == "aggregation":
        t = AggregationTranslator(sc, hdfs_path, None, mapping, model, dictionary)
    else:
        t = InjectionTranslator(sc, hdfs_path, None, mapping, model, dictionary)
    p.stop()
    return t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="ibdgc")
    ap.add_argument("--doc-type", default="participant")
    ap.add_argument("--indexer", default="aggregation")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--label", default="run")
    args = ap.parse_args()

    import tube.settings as config
    from tube.utils.spark import make_spark_context

    sc = make_spark_context(config)
    # Report straight from the SparkConf the context was built with. Reading
    # via SparkSession.conf returns Spark's runtime view, which can report
    # defaults rather than what this context was actually configured with.
    _c = sc.getConf()
    conf = {
        "spark.master": _c.get("spark.master", "(unset)"),
        "spark.sql.shuffle.partitions": _c.get(
            "spark.sql.shuffle.partitions", "(default 200)"
        ),
        "spark.sql.adaptive.enabled": _c.get(
            "spark.sql.adaptive.enabled", "(default false)"
        ),
        "spark.serializer": _c.get("spark.serializer", "(default java)"),
    }

    for i in range(args.repeat):
        t0 = time.perf_counter()
        translator = build_translator(args.schema, args.doc_type, args.indexer, sc)
        t_build = time.perf_counter() - t0

        t1 = time.perf_counter()
        df = translator.translate()
        # translate() is lazy; force materialization to time real work.
        n_rows = df.count() if df is not None else 0
        t_translate = time.perf_counter() - t1

        rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
        print(
            "BENCH "
            + json.dumps(
                {
                    "label": args.label,
                    "schema": args.schema,
                    "doc_type": args.doc_type,
                    "iteration": i,
                    "build_s": round(t_build, 3),
                    "translate_s": round(t_translate, 3),
                    "rows": n_rows,
                    "driver_rss_mb": round(rss_mb, 1),
                    "spark_master": conf["spark.master"],
                    "shuffle_partitions": conf["spark.sql.shuffle.partitions"],
                    "adaptive": conf["spark.sql.adaptive.enabled"],
                    "serializer": conf["spark.serializer"].split(".")[-1],
                }
            ),
            flush=True,
        )
    sc.stop()


if __name__ == "__main__":
    main()
