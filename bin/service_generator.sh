#!/bin/bash

# Create nginx service and its socket for a package
# This is part of cloud deployment of reporting_unified package named as
# ersa_reporting.
# This is for unified api applications.

if [ $UID -ne 0 ]; then
    echo "error: this script requires root access"
    exit 1
fi

if [[ $# -ne 2 ]]; then
    echo "error: need to know the name of package gunicorn to serve and its serving port"
    exit 1
fi

PDIR=/usr/lib/ersa_reporting
package=$1
port=$2
fullname=unified.apis.$package

cat > /usr/lib/systemd/system/gunicorn.$package.service <<EOF
[Unit]
Description=Gunicorn daemon for serving $package API
Requires=gunicorn.$package.socket
After=network.target

[Service]
Type=simple
User=nginx
Group=nginx
WorkingDirectory=$PDIR
Environment=PATH=$PDIR/unified_api_env/bin
ExecStart=$PDIR/unified_api_env/bin/gunicorn -c ${package}.conf $fullname:app
ExecReload=/bin/kill -s HUP \$MAINPID
ExecStop=/bin/kill -s TERM \$MAINPID
RuntimeDirectory=gunicorn
PrivateTmp=true
PIDFile=/run/gunicorn/$package.pid

[Install]
WantedBy=multi-user.target
EOF

cat > /usr/lib/systemd/system/gunicorn.$package.socket <<EOF
[Unit]
Description=gunicorn socket to unified.$package

[Socket]
ListenStream=/run/gunicorn/$package.socket
ListenStream=0.0.0.0:$port

[Install]
WantedBy=sockets.target
EOF

cat > /etc/nginx/conf.d/$package.conf <<EOF
server {
    listen 80;
    server_name ${package}-dev.reporting.ersa.edu.au;
    location / {
        proxy_pass http://localhost:$port;
        proxy_http_version 1.1;
        proxy_redirect off;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    }
}
EOF

cat > /etc/nginx/default.d/$package.conf <<EOF
location /$package {
    rewrite ^/$package(.*) /\$1 break;
    proxy_pass http://localhost:$port;
    proxy_http_version 1.1;
    proxy_redirect off;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
}
EOF

