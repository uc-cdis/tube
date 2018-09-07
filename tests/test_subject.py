import json
import os
from operator import itemgetter

import psycopg2
import pytest
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from psycopg2.extensions import AsIs
from psycopg2.extras import RealDictCursor

import tube.settings as config

test_data_folder = './tests/test_data'

test_files = os.listdir(test_data_folder)
# remove two "metadata" files
test_files.remove('NodeDescriptions.json')
test_files.remove('DataImportOrder.txt')
# filter out dot-files
test_files = filter(lambda x: x[0] != '.', test_files)
test_files = map(lambda x: os.path.splitext(x)[0], test_files)


def items_in_file(filename):
    in_json = '.'.join([filename, 'json'])
    in_json_path = os.path.join(test_data_folder, in_json)

    with open(in_json_path, 'r') as f:
        entries = json.load(f)
        entries = sorted(entries, key=itemgetter('submitter_id'))
    total_entries = len(entries)

    return total_entries, entries


@pytest.mark.parametrize("filename", test_files)
def test_total_number_equal_db(filename):
    total_in_files, _ = items_in_file(filename)

    conn = psycopg2.connect(config.PYDBC)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    table = AsIs("node_{table}".format(table=filename.replace('_', '')))

    cur.execute("SELECT COUNT(*) FROM %s;", (table,))
    total_subjects_in_db = cur.fetchall()[0]['count']

    assert total_in_files == total_subjects_in_db


@pytest.mark.parametrize("filename", test_files)
def test_items_equal_db(filename):
    _, entries = items_in_file(filename)

    conn = psycopg2.connect(config.PYDBC)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    table = AsIs("node_{table}".format(table=filename.replace('_', '')))

    cur.execute("SELECT _props FROM %s;", (table,))
    prop_json = cur.fetchall()
    prop_json = map(lambda item: item['_props'], prop_json)
    prop_json = sorted(prop_json, key=itemgetter('submitter_id'))

    for x, y in zip(entries, prop_json):
        keys_x = set(x.keys())
        keys_y = set(y.keys())

        keys_xy = keys_x.intersection(keys_y)

        assert len(keys_xy) != 0

        for key in keys_xy:
            assert x[key] == y[key]


def test_ensure_input_files_total():
    input_files = os.listdir(test_data_folder)
    assert len(input_files) == 53


@pytest.fixture
def total_subjects_in_file():
    in_subject_json = os.path.join(test_data_folder, 'subject.json')

    with open(in_subject_json, 'r') as f:
        subjects = json.load(f)
    total_subjects_in_file = len(subjects)

    return total_subjects_in_file


def test_subject_number_equal_file_es(total_subjects_in_file):
    es = Elasticsearch([{"host": "localhost", "port": 9200}])
    s = Search(using=es, index="etl")
    query = s.query()
    total_subjects_in_es = query.count()

    assert total_subjects_in_file == total_subjects_in_es
