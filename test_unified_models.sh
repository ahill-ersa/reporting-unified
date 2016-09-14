#!/bin/bash
set -e

# Warning: This is NOT a robust testing script.
# DO NOT OVERLY TRUST IT.

# unified Flask restful api applications need x-ersa-auth-token
# Match this with ERSA_AUTH_TOKEN in config-***.py formated as:
# ERSA_AUTH_TOKEN = "UUID STRING"

# Make sure all test configs are named as config-$package.py and created
for package in hcp hnas hpc swift xfs
do
    echo Testing models of $package
    config=config-$package.py
    echo "Config file is $config"
    export APP_SETTINGS=$config
    python -m unittest unified.models.tests.test_$package
done
