#!/bin/bash

help () {
	echo "Use $0 -pu <postgres-user> -pp <postgres-pwd> -ph <postgres-host> 
			-mu <mongo-user> -mp <mongo-pwd> -mh <mongo-host> <dir-name>"
}
if [ $# -lt 1 ]; then
    echo "Your command line contains $# arguments"
    help
    exit 0
fi
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -pu|--pg-user)
    PGUSER="$2"
    shift # past argument
    shift # past value
    ;;
    -pp|--pg-pwd)
    PGPWD="$2"
    shift # past argument
    shift # past value
    ;;
    -ph|--pg-host)
    PGHOST="$2"
    shift # past argument
    shift # past value
    ;;
    -mu|--mo-user)
    MOUSER="$2"
    shift # past argument
    shift # past value
    ;;
    -mp|--mo-pwd)
    MOPWD="$2"
    shift # past argument
    shift # past value
    ;;
    -mh|--mo-host)
    MOHOST="$2"
    shift # past argument
    shift # past value
    ;;
    -h|--help)    # help
    help
    exit 0
    ;;
    --default)
    DEFAULT=YES
    shift # past argument with no value
    ;;
    *)    # unknown option
	POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
# set -x
set -- "${POSITIONAL[@]}" # restore positional parameters
if [[ -z "$PGUSER" || -z "$PGPWD" || -z "$PGHOST" || -z "$MOUSER" || -z "$MOPWD" || -z "$MOHOST" || -z "$1" ]]
then
    help
else
	SERVICE_NAME="$1"
	YML_DIR="mosql-gem\/haystack\/$SERVICE_NAME"
    echo "PGUSER       = ${PGUSER}"
	echo "PGPWD        = ${PGPWD}"
	echo "PGHOST       = ${PGHOST}"
	echo "MOUSER       = ${MOUSER}"
	echo "MOPWD        = ${MOPWD}"
	echo "MOHOST       = ${MOHOST}"
	echo "SERVICE NAME = ${SERVICE_NAME}"
	echo "YML_DIR.     = ${YML_DIR}"
	cat etc/init/mosql-init.conf.template | sed 's/##pg_user##/'"$PGUSER"'/g; s/##pg_pwd##/'"$PGPWD"'/g; ' \
	| sed 's/##pg_url##/'"$PGHOST"'/g; s/##mongo_user##/'"$MOUSER"'/g; ' \
	| sed 's/##mongo_pwd##/'"$MOPWD"'/g; s/##mongo_url##/'"$MOHOST"'/g; ' \
	| sed 's/##yml_dir##/'"$YML_DIR"'/g; s/##service_name##/'"$1"'/g; ' \
	> etc/init/mosql-init.conf

	sudo mkdir -p /etc/init
	sudo cp etc/init/mosql-init.conf /etc/init

	# sudo initctl start mosql-init
	sudo initctl status mosql-init
fi