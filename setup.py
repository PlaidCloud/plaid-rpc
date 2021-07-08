from setuptools import setup

test_deps = [
    'pytest',
    'mock',
    'minimock',
    'pytest-cov',
    'pytest-runner',
    'hdbcli',
    'pyhdb',
]

extras = {
    'test': test_deps
}

try:
    import pygit2
    import os
    import datetime
    repo = pygit2.Repository(os.getcwd())
    commit_hash = repo.head.target
    commit = repo[commit_hash]
    print('commit {}'.format(commit_hash))
    print(datetime.date.fromtimestamp(commit.commit_time))
    print(commit.message)
except ImportError:
    print('pygit2 is not available. Cannot detect current commit.')
except:
    print('This is probably not a repo, a copy of the code.')

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='plaidcloud-rpc',
    version="1.0.1",
    author='Michael Rea',
    author_email='michael.rea@tartansolutions.com',
    packages=['plaidcloud.rpc', 'plaidcloud.rpc.connection', 'plaidcloud.rpc.remote'],
    install_requires=[
        'comtypes;platform_system=="Windows"',
        'messytables',
        'packaging',
        'psycopg2-binary',
        'requests',
        'requests_futures',
        'setuptools',
        'orjson',
        'six',
        'sqlalchemy',
        'sqlalchemy-greenplum',
        'sqlalchemy-hana',
        'toolz',
        'tornado',
        'unicodecsv',
        'urllib3',
    ],
    tests_require=test_deps,
    setup_requires=['pytest-runner'],
    extras_require=extras,
    dependency_links=[
        # 'https://github.com/PlaidCloud/sqlalchemy-greenplum/tarball/master#egg=sqlalchemy-greenplum-0.0.1',
        # 'file:///usr/sap/hdbclient/hdbcli-2.2.36.tar.gz#egg=hdbcli'
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
)
