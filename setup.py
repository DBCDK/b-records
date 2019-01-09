#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
from setuptools import setup, find_packages

## See the following pages for keywords possibilities for setup keywords, etc.
# https://packaging.python.org/
# https://docs.python.org/3/distutils/apiref.html
# https://docs.python.org/3/distutils/setupscript.html

setup(name='b-records',
      version='0.1.0',
      package_dir={'': 'src'},
      packages=find_packages(where='src'),
      description='creates b-records for content-first',
      test_suite='b_records.tests',
      provides=['b_records'],
      maintainer="ait",
      maintainer_email="shm@dbc.dk",
      zip_safe=False)
