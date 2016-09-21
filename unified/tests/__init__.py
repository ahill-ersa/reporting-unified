import os
import time
import datetime

now = int(time.time())
now_minus_24hrs = int(now - datetime.timedelta(days=1).total_seconds())


def client_get(app):
    app.testing = True
    CLIENT = app.test_client()
    HEADERS = {'x-ersa-auth-token': os.environ['auth_token']}

    def get(url):
        return CLIENT.get(url, headers=HEADERS)

    return get
