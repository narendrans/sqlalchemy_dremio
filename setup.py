#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'pyodbc>=4.0.17',
    'sqlalchemy>=1.2',
    'pyarrow'
]

setup_requirements = [
]

test_requirements = [
]

setup(
    name='sqlalchemy_dremio',
    version='1.2.1',
    description="A SQLAlchemy dialect for Dremio via the ODBC and Flight interface.",
    long_description=readme,
    long_description_content_type='text/markdown',
    author="Naren",
    author_email='me@narendran.info',
    url='https://github.com/narendrans/sqlalchemy_dremio',
    packages=find_packages(include=['sqlalchemy_dremio']),
    entry_points={
        'sqlalchemy.dialects': [
            'dremio = sqlalchemy_dremio.pyodbc:DremioDialect_pyodbc',
            'dremio.flight = sqlalchemy_dremio.flight:DremioDialect_flight',
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License",
    zip_safe=False,
    keywords='sqlalchemy_dremio',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7'
    ]
)
