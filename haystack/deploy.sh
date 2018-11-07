#!/bin/bash

echo 'hello world'
pg_user='ketan'
pg_pwd='f6JF=Uv9fXbMxS'
pg_url='test-dashboard.cgqtgkpz8uca.us-east-1.rds.amazonaws.com'
mongo_user='oploguser'
mongo_pwd='53r5rIn74ocjrwNs'
mongo_url='xxSG-haystackdbec2-1717.servers.mongodirector.com'

cat etc/init/mosql-init.conf.template | sed 's/##pg_user##/'"$pg_user"'/g; s/##pg_pwd##/'"$pg_pwd"'/g; ' \
| sed 's/##pg_url##/'"$pg_url"'/g; s/##mongo_user##/'"$mongo_user"'/g; ' \
| sed 's/##mongo_pwd##/'"$mongo_pwd"'/g; s/##mongo_url##/'"$mongo_url"'/g; ' \
> etc/init/mosql-init.conf

sudo mkdir -p /etc/init
sudo cp mosql-init.conf /etc/init

sudo initctl start mosql-init
sudo initctl status mosql-init

