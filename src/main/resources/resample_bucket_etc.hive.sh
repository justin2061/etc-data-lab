#!/bin/bash
HIVE_CLI=`which hive`
SAMPLE_ROWS=100

 echo "$HIVE_CLI -e  \"INSERT OVERWRITE LOCAL DIRECTORY '/data2/workspace/etc/resample_0509_bucket' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from etc.m06a
 TABLESAMPLE(BUCKET 365 OUT OF 3650 ON DetectionTime_O);"\";

 $HIVE_CLI -e  "INSERT OVERWRITE LOCAL DIRECTORY '/data2/workspace/etc/resample_0509_bucket' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from etc.m06a TABLES
AMPLE(BUCKET 365 OUT OF 3650 ON DetectionTime_O);";