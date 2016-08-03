import os

HEADERS = {'x-ersa-auth-token': os.environ['auth_token']}


def client_get(app):
    app.testing = True
    CLIENT = app.test_client()

    def get(url):
        return CLIENT.get(url, headers=HEADERS)

    return get
