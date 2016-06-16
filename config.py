# These should be set

ERSA_REPORTING_PACKAGE = "YOUR_DB"
ERSA_AUTH_TOKEN = "sometoken"
SQLALCHEMY_DATABASE_URI = "postgresql://apiuser:YOUR_PASS@localhost/YOUR_DB"

# These are optional
LOG_DIR = "."
LOG_LEVEL = logging.DEBUG
LOG_SIZE = 30000000
# 20160720: flask-sqlalchemy support of SQLALCHEMY_BINDS is questionable,
# you may need patch
SQLALCHEMY_BINDS = {
    "ANOTHER_DIND": "postgresql://apiuser:YOUR_PASS@localhost/YOUR_ANOTHER_DB"
}
