# Transform-stage benchmark

Measures the tube translator against the `tests/dataframe_tests` fixtures. No
Postgres, Sqoop, HDFS or Elasticsearch required, so it isolates the stage that
the Spark tuning actually affects.

## Running

Both arms must run in **separate containers** — `SparkContext.getOrCreate()`
reuses an existing context, so two configs in one process silently measure the
same thing.

`PYTHONPATH=/work` is **required**. Running a script from a file puts the
script's directory on `sys.path[0]`, not the CWD, so without it Python imports
the image's baked-in `/tube/tube` instead of the mounted working copy — and you
benchmark the stock image twice.

```sh
cat > /tmp/arm.sh <<'SH'
/venv/bin/python -B tools/bench/bench_transform.py \
  --schema ibdgc --doc-type participant --repeat 6 --label "$LBL" \
  2>/dev/null | grep "^BENCH"
SH

# baseline: the settings master shipped
docker run --rm -v "$PWD":/work -v /tmp/arm.sh:/arm.sh -w /work \
  -e PYTHONPATH=/work -e RUNNING_MODE=Test -e LBL=baseline \
  -e SPARK_MASTER='local[1]' -e SPARK_SHUFFLE_PARTITIONS=200 \
  -e SPARK_ADAPTIVE_ENABLED=false \
  -e SPARK_SERIALIZER=org.apache.spark.serializer.JavaSerializer \
  quay.io/cdis/tube:master bash /arm.sh > /tmp/a.txt

# optimized: current defaults
docker run --rm -v "$PWD":/work -v /tmp/arm.sh:/arm.sh -w /work \
  -e PYTHONPATH=/work -e RUNNING_MODE=Test -e LBL=optimized \
  quay.io/cdis/tube:master bash /arm.sh > /tmp/b.txt

cat /tmp/a.txt /tmp/b.txt | grep BENCH | sed 's/^BENCH //' \
  > tools/bench/results/ibdgc-participant.jsonl
python3 tools/bench/analyze.py
```

Run the arms **sequentially on an idle machine**. Concurrent containers
penalise the multi-core arm specifically and understate the difference — early
runs of this benchmark reported 2.0-2.4x purely from contention, against 4.2x
measured cleanly.

## Results: 2026-09-22, 8-core / 11.7GB Docker

`ibdgc` / `participant` aggregator, 190 output documents, 6 iterations per arm
(iteration 0 dropped as JVM warmup).

| arm       | master    | shuffle | AQE | serializer | cold  | min   | median | max   | stdev |
|-----------|-----------|---------|-----|------------|-------|-------|--------|-------|-------|
| baseline  | local[1]  | 200     | off | Java       | 44.79 | 24.81 | 26.45  | 34.09 | 3.86  |
| optimized | local[*]  | 16      | on  | Kryo       | 18.46 | 5.04  | 6.25   | 6.68  | 0.62  |

- median **26.45s -> 6.25s = 4.24x** (worst case 3.72x)
- ranges do not overlap, so the difference is unambiguous
- cold start 44.79s -> 18.46s (2.4x)
- both arms emit 190 rows — output unchanged
- optimized is also ~6x more stable (stdev 0.62 vs 3.86): fewer, larger tasks
  means less scheduling jitter

### What this does not cover

- **Sqoop import and the Elasticsearch write.** `coalesce(1)` removal and the
  `es.batch.*` tuning contribute nothing to these numbers; they need an
  end-to-end harness with Postgres and ES, and should be additional wins.
- **Realistic data volume.** The fixtures are ~10k rows total, largest node
  table 97 rows. The win measured here is removal of per-task overhead. On
  larger data `local[*]` has real parallel work to spread, so the direction
  should hold, but the magnitude needs synthetic data to confirm.
