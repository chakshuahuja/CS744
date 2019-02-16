#!/bin/sh

~/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class "PageRankCached" --master spark://$2:7077 --driver-memory 8G --executor-memory 8G  target/scala-2.11/pagerankapp_2.11-1.0.jar/ $2 $3 $4 --deploy-mode cluster
