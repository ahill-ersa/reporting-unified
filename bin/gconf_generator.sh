#!/bin/bash

# This is part of cloud deployment of reporting_unified package named as
# ersa_reporting.
# Should run after non-secret configuration files have been installed.
# This is for unified api applications.

if [ $UID -ne 0 ]; then
    echo "error: this script requires root access"
    exit 1
fi

if [[ $# -ne 1 ]]; then
    echo "error: missing name of package gunicorn to serve"
    exit 1
fi

PDIR=/usr/lib/ersa_reporting
package=$1
echo "Will create $PDIR/$package.conf"
cat > $PDIR/$package.conf <<EOF
timeout = 7200
proc_name = "$package"
workers = 2
PDIRfile = "/run/gunicorn/$package.PDIR"
raw_env = ["APP_SETTINGS=config-${package}.py"]
accesslog = "/var/log/gunicorn/${package}_access.log"
errorlog = "/var/log/gunicorn/${package}_error.log"
loglevel = "info"
EOF

echo "Running: systemctl start gunicorn.$package.socket"
systemctl enable gunicorn.$package.socket
systemctl start gunicorn.$package.socket
systemctl status gunicorn.$package.socket
