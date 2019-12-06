import hashlib
import json
import os
import zipfile

import boto3
import requests

import tube.settings as config
from tube.etl.plugins import post_process_plugins, add_auth_resource_path_mapping
from tube.etl.spark_base import SparkBase


def json_export(x, doc_type):
    x[1]['{}_id'.format(doc_type)] = x[0]
    x[1]['node_id'] = x[0]  # redundant field for backward compatibility with arranger
    return (x[0], json.dumps(x[1]))


def archive_export(x):
    # Write to zip archive...
    local_filepath = '{}/{}.zip'.format(config.LOCAL_ARCHIVE_DIR.rstrip('/'), x[0])
    s3_filepath = '{}/{}.zip'.format(config.S3_ARCHIVE_DIR.rstrip('/'), x[0])
    with zipfile.ZipFile(local_filepath, mode='w') as zf:
        zf.writestr(x[0], x[1])

    # Upload to S3...
    boto_session = boto3.Session(
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.S3_ARCHIVE_BUCKET_REGION_NAME,
    )
    s3 = boto_session.resource('s3')
    s3.Bucket(config.S3_ARCHIVE_BUCKET_NAME).upload_file(local_filepath, s3_filepath)

    # ...and index in indexd
    s3url = 'https://{bkt}.s3-{rgn}.amazonaws.com/{pth}'.format(
        bkt=config.S3_ARCHIVE_BUCKET_NAME,
        rgn=config.S3_ARCHIVE_BUCKET_REGION_NAME,
        pth=s3_filepath,
    )
    auth_resource_path = json.loads(x[1])['auth_resource_path']
    authz = auth_resource_path if type(auth_resource_path)==list else [auth_resource_path]
    data = {
        "urls": [s3url],
        "form": "object",
        "hashes": {
            "sha256": hashlib.sha256(x[1]).hexdigest()
        },
        "size": len(x[1]), # In python 3, change to len(x[1].encode("utf8"))
        "file_name": "{}.zip".format(x[0]), # TODO should file_name be the entire file path or just the x.zip?
        "authz": authz,
    }
    resp = requests.post(
        '{}/index/'.format(config.INDEXD),
        headers={"content-type": "application/json"},
        auth=requests.auth.HTTPBasicAuth(config.INDEXD_BASICAUTH_USERNAME, config.INDEXD_BASICAUTH_PASSWORD),
        data=json.dumps(data),
    )


class Writer(SparkBase):
    def __init__(self, sc, config):
        super(Writer, self).__init__(sc, config)

    def write_archive(self, df, doc_type, types):
        try:
            for plugin in post_process_plugins:
                df = df.map(lambda x: plugin(x))

            types = add_auth_resource_path_mapping(types)

            df = df.map(lambda x: json_export(x, doc_type))

            local_archive_dir = config.LOCAL_ARCHIVE_DIR.rstrip('/')

            # When we switch to Python 3 use this:
            # os.makedirs(local_archive_dir, exist_ok=True)
            if not os.path.exists(local_archive_dir):
                os.makedirs(local_archive_dir)

            df.foreach(archive_export)

        except Exception as e:
            print(e)
