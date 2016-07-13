#!/bin/bash
PYTHON_FILE=$1
SQL_FILE=$2
OUTPUT=$3
LOCAL_OUTPUT=$4
NUM_EXECS=$5
NAME="buscapstone"

HD=hadoop
SP=spark-submit

$HD fs -rm -r -skipTrash $OUTPUT
$SP --name "$NAME" --packages com.databricks:spark-csv_2.11:1.4.0 --num-executors $NUM_EXECS --files $SQL_FILE $PYTHON_FILE $SQL_FILE $OUTPUT
rm -f $LOCAL_OUTPUT
$HD fs -cat $OUTPUT/part* > $LOCAL_OUTPUT
