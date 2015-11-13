#!/usr/bin/env python3

# pylint: disable=missing-docstring

import glob

from setuptools import setup

scripts = [x for x in glob.glob("bin/ersa-reporting-*")
           if x != "bin/ersa-reporting-api"]

setup(name="ersa-reporting",
      version="0.1.0",
      install_requires=["flask>=0.10.1", "flask-restful", "flask-cors",
                        "flask-sqlalchemy", "psycopg2", "streql"],
      packages=["ersa_reporting"],
      scripts=scripts)
