#!/bin/bash

yum -y install epel-release
yum -y update
yum -y install python-pip gcc nginx vim screen
yum -y install python34 python34-devel postgresql-devel

pip install pip --upgrade
pip install --upgrade virtualenv

# install package
PDIR=/usr/lib/ersa_reporting
mkdir $PDIR
cd $PDIR
virtualenv -p python3 unified_api_env

source unified_api_env/bin/activate
# install the latest commit
pip install --no-cache-dir gunicorn https://github.com/eResearchSA/reporting-unified/archive/master.tar.gz
deactivate
chown -R nginx:nginx $PDIR

function confs {
package=$1
port=$2
fullname=ersa_reporting.$package

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
Description=gunicorn socket to ersa_reporting.reporting-unified.nova

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
}

ERSA_REPORTING_PACKAGE=nova
fullname=ersa_reporting.$ERSA_REPORTING_PACKAGE
confs $ERSA_REPORTING_PACKAGE 9000

iip=`curl -s http://169.254.169.254/2009-04-04/meta-data/local-ipv4`
echo "$iip ${ERSA_REPORTING_PACKAGE}-dev.reporting.ersa.edu.au ${ERSA_REPORTING_PACKAGE}-dev" >> /etc/hosts

mkdir /var/log/gunicorn
chown nginx:nginx /var/log/gunicorn

systemctl enable nginx
systemctl start nginx
systemctl enable gunicorn.$ERSA_REPORTING_PACKAGE.socket

echo "Instance bootstrap completed. Need to install the conf file of gunicorn"
