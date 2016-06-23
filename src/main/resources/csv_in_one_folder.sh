#!/bin/bash
FOLDER=$1

for f in $(find $1 -name \*.csv -print); do
 echo "File -> $f"
 mv $f all_csv/
done