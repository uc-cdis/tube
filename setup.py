import toml
from setuptools import setup, find_packages
from pathlib import Path

pyproject = toml.load(Path(__file__).parent / 'pyproject.toml')
dependencies = pyproject.get('project', {}).get('dependencies', [])
if not dependencies:
    dependencies = pyproject.get('tool', {}).get('poetry', {}).get('dependencies', {}).keys()

setup(
    name="tube",
    version="1.0.5",
    packages=find_packages(),
    install_requires=list(dependencies),
    entry_points={
        'console_scripts': [
            # 'my-script=my_package.module:main',
        ],
    },
    description="Spark ETL of Gen3",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/uc-cdis/tube",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved ::  Apache License 2.0",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)