#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import dynamobnr

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('requirements.txt') as requirements_file:
    requirements = requirements_file.readlines()

setup(
    name=dynamobnr.__name__,
    version=dynamobnr.__version__,
    description=dynamobnr.__doc__.strip(),
    long_description=readme,
    author=dynamobnr.__author__,
    author_email=dynamobnr.__email__,
    url='https://github.com/XaF/dynamodb-bnr',
    packages=[
        dynamobnr.__name__,
        '{}/commands'.format(dynamobnr.__name__),
    ],
    install_requires=requirements,
    license="Apache License",
    zip_safe=True,
    keywords='pip requirements imports',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Topic :: Database',
        'Topic :: Utilities',
    ],
    entry_points={
        'console_scripts': [
            'dynamobnr=dynamobnr.dynamobnr:cli',
            'dynamodb-bnr=dynamobnr.dynamobnr:cli',
        ],
    },
)
