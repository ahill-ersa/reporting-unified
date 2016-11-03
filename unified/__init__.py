import os
import sys
import logging
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

if 'APP_SETTINGS' not in os.environ:
    sys.exit('Missing APP_SETTINGS envrionment variable')

LOG_FORMAT = '%(asctime)s %(levelname)s %(module)s %(filename)s %(lineno)d: %(message)s'
SAN_MS_DATE = '%Y-%m-%d %H:%M:%S'
LOG_FORMATTER = logging.Formatter(LOG_FORMAT, SAN_MS_DATE)

app = Flask("app")
app.config.from_envvar('APP_SETTINGS')

db = SQLAlchemy(app)
