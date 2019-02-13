#!/bin/sh
echo "Input File name: $1"
echo "Output File name: $2"
echo
echo "Starting Spark Job"
spark-shell --master spark://128.104.223.139:7077 -i sort_csv.sc --executor-memory 8G --driver-memory 8G  --executor-cores 5  --conf spark.app.inputFile="$1" --conf spark.app.outputFile="$2"
