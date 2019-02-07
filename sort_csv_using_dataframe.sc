// Using DataFrame/Dataset API

// Use filename as `file:///users/chakshu/export.csv` if you want to read from local rather than HDFS cluster

val inputDF = spark.read.option("header", true).option("inferSchema", true).csv("/export.csv")
val outputDF = inputDF.sort("cca2", "timestamp")
outputDF.repartition(1).write.option("header", true).csv("/hdfssorted.csv")