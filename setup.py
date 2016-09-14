#!/usr/bin/env python3

# pylint: disable=missing-docstring

import glob

from setuptools import setup

setup(name="ersa-reporting",
      version="2.0.0",
      install_requires=["flask>=0.10.1", "flask-restful", "flask-cors",
                        "flask-sqlalchemy", "psycopg2", "requests", "arrow",
                        "python-keystoneclient", "python-novaclient"],
      py_modules=["nectar"],
      packages=["unified", "unified.apis", "unified.models"],
      scripts=["bin/gconf_generator.sh",
               "bin/service_generator.sh",
               "bin/ersa-reporting-prep-tables",
               "bin/ersa-reporting-ingest"])
