#!/usr/bin/env bash

# validate our username
echo $1 | grep -E -q '^[a-z]+$' || (echo "Only a-z allowed for name, $1 provided" && exit 1)

# validate username
if [ -z "$2" ]; then
    echo "No Redshift username supplied"
    exit 1
fi

# validate password
if [ -z "$3" ]; then
    echo "No Redshift password supplied"
    exit 1
fi

# generate our schema.yml file via envsubst
export ML_SCHEMA="ml-$1"
export REDSHIFT_USERNAME=$2
export REDSHIFT_PASSWORD=$3
envsubst < ./scripts/profiles.yml > ~/.dbt/profiles.yml

echo "~/.dbt/profiles.yml generated"