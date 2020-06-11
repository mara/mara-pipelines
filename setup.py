from setuptools import setup, find_packages
import re

def get_long_description():
    with open('README.md') as f:
        return re.sub('!\[(.*?)\]\(docs/(.*?)\)', r'![\1](https://github.com/mara/mara-pipelines/raw/master/docs/\2)', f.read())

setup(
    name='mara-pipelines',
    version='3.0.0',

    description='Opinionated lightweight ELT pipeline framework',

    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    url = 'https://github.com/mara/mara-pipelines',

    install_requires=[
        'mara-db>=4.2.0',
        'mara-page>=1.3.0',
        'graphviz>=0.8',
        'python-dateutil>=2.6.1',
        'pythondialog>=3.4.0',
        'more-itertools>=3.1.0',
        'psutil>=5.4.0',
        'wheel>=0.31',
        'requests>=2.19.1'
    ],

    python_requires='>=3.6',

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
)

