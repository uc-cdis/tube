import itertools
import json
import os
from operator import itemgetter

test_data_folder = "./tests/test_data"


def items_in_file(filename):
    in_json = ".".join([filename, "json"])
    in_json_path = os.path.join(test_data_folder, in_json)

    with open(in_json_path, "r") as f:
        entries = json.load(f)
        entries = sorted(entries, key=itemgetter("submitter_id"))
    total_entries = len(entries)

    return filename, total_entries, entries


def get_test_files():
    test_files = os.listdir(test_data_folder)
    # remove two "metadata" files
    test_files.remove("DataImportOrder.txt")
    # filter out dot-files
    test_files = [x for x in test_files if x[0] != "."]
    test_files = map(lambda x: os.path.splitext(x)[0], test_files)

    return test_files


test_files = get_test_files()


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, b)
