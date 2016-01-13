#!/usr/bin/env python3

# pylint: disable=missing-docstring

import glob

from setuptools import setup

setup(name="ersa-reporting",
      version="1.2.2",
      install_requires=["flask>=0.10.1", "flask-restful", "flask-cors",
                        "flask-sqlalchemy", "psycopg2", "requests"],
      packages=["ersa_reporting"],
      scripts=glob.glob("bin/ersa-reporting-*"))
