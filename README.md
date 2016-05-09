# reporting-unified [![Build Status](https://travis-ci.org/eResearchSA/reporting-unified.svg)](https://travis-ci.org/eResearchSA/reporting-unified)
A bunch of small Flask applications for accessing eRSA reporting databases.

##Deployment

The package can be served by, for example, __nginx__ (proxy) + __gunicorn__.

The log of an application is currently hard-coded to be saved in `/var/log/gunicorn/` which assumes `gunicorn` is configured to save logs to there.

An application's log is named as ersa_reporting._application_.log, e.g. __'ersa_reporting.hnas.log'__.

One example shown here assumes package has been installed in `/usr/lib/ersa_reporting` in a virtual environment in `unified_api_env`
and application `hnas` is being served:

###run gunicorn

```shell
PDIR=/usr/lib/ersa_reporting
cd $PDIR
source unified_api_env/bin
unified_api_env/bin/gunicorn -c hnas.conf hnas:app
```

__conf file `hnas.conf`__

```python

raw_env = ["ERSA_REPORTING_PACKAGE=hnas", "ERSA_DEBUG=True",
           "ERSA_DATABASE_URI=postgresql://user:pass@host/db",
           "ERSA_AUTH_TOKEN=DEBUG_TOKEN"]
timeout = 7200
proc_name = "hnas"
workers = 2
pidfile = "/run/gunicorn/hnas.pid"
accesslog = "/var/log/gunicorn/hnas_access.log"
errorlog = "/var/log/gunicorn/hnas_error.log"
loglevel = "info"
```
