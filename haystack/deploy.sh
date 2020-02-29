#!/bin/bash
if [ $# -lt 1 ]; then
    echo "Your command line contains $# arguments"
    echo "Use $0 [dir_name]"
    exit 0
fi
service_name=$1
set -x
echo 'hello world'

# TEST CREDENTIALS
pg_user='mosqluser'
pg_pwd='hzMzH2wnax9gTeY5'
pg_url='test-dashboard.cgqtgkpz8uca.us-east-1.rds.amazonaws.com'
mongo_user='oploguser'
mongo_pwd='53r5rIn74ocjrwNs'
mongo_url='SG-haystackdbec2-1718.servers.mongodirector.com'
yml_dir="mosql-gem\/haystack\/$service_name"

# PROD CREDENTIALS
# pg_user='mosqluser'
# pg_pwd='hzMzH2wnax9gTeY5'
# pg_url='dashboarddbinstance.cgqtgkpz8uca.us-east-1.rds.amazonaws.com'
# mongo_user='oploguser'
# mongo_pwd='53r5rIn74ocjrwNs'
# mongo_url='SG-haystackdbec2-1717.servers.mongodirector.com,SG-haystackdbec2-1716.servers.mongodirector.com'
# yml_dir="mosql-gem\/haystack\/$service_name"

cat etc/init/mosql-init.conf.template | sed 's/##pg_user##/'"$pg_user"'/g; s/##pg_pwd##/'"$pg_pwd"'/g; ' \
| sed 's/##pg_url##/'"$pg_url"'/g; s/##mongo_user##/'"$mongo_user"'/g; ' \
| sed 's/##mongo_pwd##/'"$mongo_pwd"'/g; s/##mongo_url##/'"$mongo_url"'/g; ' \
| sed 's/##yml_dir##/'"$yml_dir"'/g; s/##service_name##/'"$service_name"'/g; ' \
> etc/init/mosql-init.conf

sudo mkdir -p /etc/init
sudo cp etc/init/mosql-init.conf /etc/init

# sudo initctl start mosql-init
sudo initctl status mosql-init

