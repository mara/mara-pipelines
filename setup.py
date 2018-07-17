from setuptools import setup, find_packages

setup(
    name='data-integration',
    version='1.2.0',

    description='Opinionated lightweight ETL pipeline framework',

    install_requires=[
        'mara-db>=3.0.0',
        'mara-page>=1.3.0',
        'graphviz>=0.8',
        'python-dateutil>=2.6.1',
        'pythondialog>=3.4.0',
        'more-itertools>=3.1.0',
        'psutil>=5.4.0',
        'wheel>=0.31',
        'requests'>=2.19.1
    ],

    dependency_links=[
        'git+https://github.com/mara/mara-page.git@1.3.0#egg=mara-page-1.3.0',
        'git+https://github.com/mara/mara-db.git@3.0.0#egg=mara-db-3.0.0'
    ],

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
)

