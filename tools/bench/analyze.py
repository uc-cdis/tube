"""Summarise bench_transform.py output.

Usage: python tools/bench/analyze.py [results.jsonl ...]
Reports the spread, not just a median -- Spark run-to-run variance on small
datasets is wide enough that a single median is misleading.
"""
import json
import statistics as st
import sys

paths = sys.argv[1:] or ["tools/bench/results/ibdgc-participant.jsonl"]
rows = []
for p in paths:
    for line in open(p):
        line = line.strip()
        if not line:
            continue
        if line.startswith("BENCH "):
            line = line.split("BENCH ", 1)[1]
        rows.append(json.loads(line))
if not rows:
    sys.exit("no results found in: " + ", ".join(paths))

arms = []
for r in rows:
    if r["label"] not in arms:
        arms.append(r["label"])

print("CONFIG ACTUALLY USED")
print(f"{'arm':<11}{'master':<10}{'shuffle':<9}{'aqe':<7}{'serializer':<16}{'rows':<6}")
for lbl in arms:
    a = [x for x in rows if x["label"] == lbl][0]
    print(
        f"{lbl:<11}{a['spark_master']:<10}{a['shuffle_partitions']:<9}"
        f"{a['adaptive']:<7}{a['serializer']:<16}{a['rows']:<6}"
    )

print("\nTRANSLATE TIME (s) -- iteration 0 dropped as JVM/filesystem warmup")
print(f"{'arm':<11}{'cold':<8}{'n':<4}{'min':<8}{'median':<9}{'max':<8}{'stdev':<8}")
summary = {}
for lbl in arms:
    t = [x["translate_s"] for x in rows if x["label"] == lbl]
    warm = t[1:] or t
    summary[lbl] = warm
    sd = st.stdev(warm) if len(warm) > 1 else 0.0
    print(
        f"{lbl:<11}{t[0]:<8.2f}{len(warm):<4}{min(warm):<8.2f}"
        f"{st.median(warm):<9.2f}{max(warm):<8.2f}{sd:<8.2f}"
    )

if len(arms) == 2:
    b, o = summary[arms[0]], summary[arms[1]]
    mb, mo = st.median(b), st.median(o)
    print(f"\n{arms[0]} -> {arms[1]}")
    print(f"  median  {mb:.2f}s -> {mo:.2f}s   {(mb-mo)/mb*100:+.1f}%   {mb/mo:.2f}x")
    # Worst case for the optimized arm vs best case for the baseline: the most
    # conservative reading of the same data.
    print(
        f"  worst-case  {min(b):.2f}s -> {max(o):.2f}s   "
        f"{(min(b)-max(o))/min(b)*100:+.1f}%   {min(b)/max(o):.2f}x"
    )
    if max(o) < min(b):
        print("  ranges do not overlap: the difference is unambiguous")
    else:
        print("  WARNING: ranges overlap -- collect more samples")
