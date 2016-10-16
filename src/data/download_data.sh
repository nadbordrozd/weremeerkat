#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR
cd ../../
source .env
cd data/raw

kg download --verbose -u $KAGGLE_USER -p $KAGGLE_PASSWORD -c outbrain-click-prediction

for f in *.zip
do
    unzip $f
done
