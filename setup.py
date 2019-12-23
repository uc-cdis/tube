from setuptools import setup, find_packages

setup(
    name='tube',
    version='0.0.1',
    install_requires=[
        "cryptography>=2.1.2",
        "dictionaryutils~=3.0.0",
        "hdfs==2.1.0",
        "gen3datamodel~=3.0.0",
        "psqlgraph~=3.0.0",
        "psycopg2~=2.8.4",
        "pyspark~=2.4.4",
        "requests>=2.18.0<3.0.0",
        "setuptools==36.6.0",
        "six~=1.12.0",
        "Werkzeug==0.15.3",
    ],
    packages=find_packages(),
)
