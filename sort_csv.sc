val inputFilename = sc.getConf.get("spark.app.inputFile", "/export.csv")
val outputFilename = sc.getConf.get("spark.app.outputFile", "/sortedexport")

val inputDF = spark.read.option("header", true).option("inferSchema", true).csv(inputFilename)
val outputDF = inputDF.sort("cca2", "timestamp")
outputDF.repartition(1).write.mode("overwrite").option("header", true).csv(outputFilename)

System.exit(0)