from setuptools import setup, find_packages
import re

def get_long_description():
    with open('README.md') as f:
        return re.sub('!\[(.*?)\]\(docs/(.*?)\)', r'![\1](https://github.com/mara/mara-pipelines/raw/master/docs/\2)', f.read())

def get_version(rel_path):
    with open(rel_path) as fp:
        for _, line in enumerate(fp):
            if line.startswith('__version__'):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]
        else:
            raise RuntimeError("Unable to find version string.")

setup(
    name='mara-pipelines',
    version=get_version('mara_pipelines/__init__.py'),

    description='Opinionated lightweight ELT pipeline framework',

    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    url = 'https://github.com/mara/mara-pipelines',

    install_requires=[
        'mara-db>=4.7.1',
        'mara-page>=1.3.0',
        'mara-storage>=0.9.0',
        'deprecation>=2.1.0',
        'graphviz>=0.8',
        'python-dateutil>=2.6.1',
        'pythondialog>=3.4.0',
        'more-itertools>=3.1.0',
        'psutil>=5.4.0',
        'wheel>=0.31',
        'requests>=2.19.1'
    ],

    setup_requires=['setuptools_scm'],
    include_package_data=True,

    python_requires='>=3.6',

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
)

