#!/usr/bin/env python3

# pylint: disable=missing-docstring

import glob

from setuptools import setup

setup(name="ersa-reporting",
      version="1.3.1",
      install_requires=["flask>=0.10.1", "flask-restful", "flask-cors", "boto",
                        "flask-sqlalchemy", "psycopg2", "requests", "arrow"],
      packages=["ersa_reporting"],
      scripts=glob.glob("bin/ersa-reporting-*"))
