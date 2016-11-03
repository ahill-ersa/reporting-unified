#!/bin/bash

# Set up an unified API application in Nectar start up script

yum -y install epel-release
yum -y update
yum -y install python-pip gcc nginx vim screen
yum -y install python34 python34-devel postgresql-devel

pip install pip --upgrade
pip install --upgrade virtualenv

iip=`curl -s http://169.254.169.254/2009-04-04/meta-data/local-ipv4`
echo "$iip ${ERSA_REPORTING_PACKAGE}-dev.reporting.ersa.edu.au ${ERSA_REPORTING_PACKAGE}-dev" >> /etc/hosts

mkdir /var/log/gunicorn
chown nginx:nginx /var/log/gunicorn

mkdir /run/gunicorn

systemctl enable nginx
systemctl start nginx

# install package
PDIR=/usr/lib/ersa_reporting
mkdir $PDIR
cd $PDIR
chown -R nginx:nginx $PDIR

virtualenv -p python3 unified_api_env
source unified_api_env/bin/activate
# install the latest commit from github.com
# pip install --no-cache-dir gunicorn https://github.com/eResearchSA/reporting-unified/archive/master.tar.gz

# install from local source, e.g. from ec2-user home
pip install --no-cache-dir gunicorn
pip install /home/ec2-user/reporting-unified

ERSA_REPORTING_PACKAGE=nova
fullname=unified.$ERSA_REPORTING_PACKAGE
service_generator.sh $ERSA_REPORTING_PACKAGE 9000
gconf_generator.sh $ERSA_REPORTING_PACKAGE
deactivate

echo "Instance bootstrap completed. Need to generate config py of the app service"
