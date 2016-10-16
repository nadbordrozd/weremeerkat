#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR
cd ../../data/interim

sqlite3 database.db < ../../src/data/into_sqlite.sql
