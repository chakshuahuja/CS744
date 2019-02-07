//Using RDD


val textData = spark.read.textFile("/web-BerkStan.txt").rdd
val  lines = textData.filter(!_.startsWith("#"))

val links = lines.map{ s =>
                val parts = s.split("\\s+")
                (parts(0), parts(1))
            }.distinct().groupByKey().cache()

var ranks = links.mapValues(v => 1.0)

for (i <- 1 to 10) {
  val contributions = links.join(ranks).values.flatMap{ case (urls, rank) =>
    val size = urls.size
    urls.map(url => (url, rank / size))
  }
  ranks = contributions.reduceByKey((x,y) => x+y).mapValues(sum => 0.15 + 0.85 * sum)
}

val finalRank = ranks.collect()
finalRank.foreach(pair => println(s"PageRank of ${pair._1} is ${pair._2} ."))

