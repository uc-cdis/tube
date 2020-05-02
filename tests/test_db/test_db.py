from operator import itemgetter

import pytest
from psycopg2.extensions import AsIs

from tests.utils import test_files, items_in_file
from tube.utils.db import get_db_cursor


@pytest.mark.parametrize("filename", test_files)
def test_total_number_equal_db(filename):
    _, total_in_files, _ = items_in_file(filename)

    with get_db_cursor("db") as cur:
        table = AsIs("node_{table}".format(table=filename.replace("_", "")))

        cur.execute("SELECT COUNT(*) FROM %s;", (table,))
        total_subjects_in_db = cur.fetchall()[0]["count"]

        assert total_in_files == total_subjects_in_db


@pytest.mark.parametrize(
    "filename,total,entries", [items_in_file(f) for f in test_files]
)
def test_items_equal_db(filename, total, entries):
    with get_db_cursor("db") as cur:
        table = AsIs("node_{table}".format(table=filename.replace("_", "")))

        cur.execute("SELECT _props FROM %s;", (table,))

        prop_json = cur.fetchall()
        prop_json = [item["_props"] for item in prop_json]
        prop_json = sorted(prop_json, key=itemgetter("submitter_id"))

        for x, y in zip(entries, prop_json):
            keys_x = set(x.keys())
            keys_y = set(y.keys())

            keys_xy = keys_x.intersection(keys_y)

            assert len(keys_xy) != 0

            for key in keys_xy:
                assert x[key] == y[key]
