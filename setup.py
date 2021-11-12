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

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='plaidcloud-rpc',
    version="1.0.1",
    author='Michael Rea',
    author_email='michael.rea@tartansolutions.com',
    packages=['plaidcloud.rpc', 'plaidcloud.rpc.connection', 'plaidcloud.rpc.remote'],
    install_requires=required,
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
