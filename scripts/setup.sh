#!/usr/bin/env bash

# create profiles.yml
echo "Setting up profiles.yml for dbt..."
./scripts/generate_profile.sh "$@"

# install mole
brew tap davrodpin/homebrew-mole && brew install mole

# setup instawork tunnel connection
mole add alias local instawork-dw \
    --source :31338 \
    --destination instawork-dw.cvgakvku4dlq.us-west-2.redshift.amazonaws.com:5439 \
    --server ec2-user@bastion.instawork.com:22
