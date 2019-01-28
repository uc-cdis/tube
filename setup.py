from setuptools import setup, find_packages

setup(
    name='tube',
    version='0.0.1',
    install_requires=[
        "cryptography>=2.1.2",
        "hdfs==2.1.0",
        "psycopg2==2.7.3.2",
        "pyspark==2.3.1",
        "python_dateutil==2.6.1",
        "requests>=2.18.0<3.0.0",
        "setuptools==36.6.0",
        "six==1.11.0",
        "Werkzeug==0.12.2",
        "psutil==2.1.3",
        "pyyaml",
        "memory_profiler"
    ],
    packages=find_packages(),
)
