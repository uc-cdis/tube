from setuptools import setup, find_packages

setup(
    name='tube',
    version='0.0.1',
    install_requires=[
        "cryptography>=2.1.2",
        "Flask>=0.10.1,<1.0.0",
        "Flask-CORS>=3.0.3,<4.0.0",
        "flask-restful == 0.3.6",
        "hdfs==2.1.0",
        "psycopg2==2.7.3.2",
        "pyspark==2.3.1",
        "pytest-flask==0.10.0",
        "pytest==3.2.3",
        "python_dateutil==2.6.1",
        "requests>=2.18.0<3.0.0",
        "setuptools==36.6.0",
        "six==1.11.0",
        "Werkzeug==0.12.2",
        "psutil==2.1.3",
        "pyyaml"
    ],
    scripts=[
        "bin/fence-create",
    ],
    packages=find_packages(),
)
