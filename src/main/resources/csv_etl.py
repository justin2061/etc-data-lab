#!/bin/bash
PYTHON=`which python`
echo $PYTHON

for f in $(find . -name \*.csv -print); do
 echo "File -> $f"
 $PYTHON csv_etl.py $f
done
[hadoop@pc-hadoop1:/data2/workspace/etc]$ more csv_etl.py
# -*- coding: utf-8 -*-
import csv
import mysql.connector
#import time
import sys
from datetime import date, datetime, timedelta

#print sys.argv
if len(sys.argv) < 2:
    print 'no csv filename.'
    sys.exit()

#print sys.argv[1]

add_m06a_data = ("insert into M06A(VehicleType, DetectionTime_O, GantryID_O, DetectionTime_D, GantryID_D, TripLength, TripEnd, TripInformation) values (%s, %s, %s, %s,
%s, %s, %s, %s)")

cnx = mysql.connector.connect(user='etc_user', password='1qaz!QAZ', host='10.64.32.48', database='etc')
cursor = cnx.cursor()
#csv_filepath = 'D:/etc/all_csv/TDCS_M06A_20140101_000000.csv'
csv_filepath = sys.argv[1]
f = open(csv_filepath, 'r')
try:
    for row in csv.reader(f):
        #print row[1]
        r1 = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        r3 = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')
        m06a_data = (row[0], r1, row[2], r3, row[4], row[5], row[6], row[7])
        #print m06a_data
        cursor.execute(add_m06a_data, m06a_data)
        cnx.commit()
except Exception as e:
    print(e)
cursor.close()
f.close()
cnx.close()