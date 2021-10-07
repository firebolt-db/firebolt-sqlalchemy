#!/bin/sh

# Author : Apurva Anand
# Script for continuous integration of firebolt-sqlalchemy project
# 

USERNAME=$1
PASSWORD=$2
ENGINE_NAME=$3
DB_NAME=$4

export PYTHONPATH=/Users/apurva.anand/firebolt_workspace/firebolt-sqlalchemy/src

# Using python. Create external and fact table. Insert data as well.
echo "Creating table for unit test"
python ci/firebolt_ingest_data.py $USERNAME $PASSWORD $DB_NAME
echo "Table for unit test has been created"

# Call pytest files to run unit tests
echo "Running unit tests"
python -m pytest tests
echo "Unit tests completed"

# Run pylint
echo "Running pylint check"
pylint src/firebolt_db | grep 'Your code has been rated at'
echo "Pylint check completed"