abstract class Data {
  import org.apache.spark.rdd.RDD
  def fileNames: Seq[String]

  def rdd: RDD[String] = fileNames
    .map(spark.read.textFile(_).rdd)
    .reduce((rdd1, rdd2) => rdd1.union(rdd2))
}

object EnWikiData extends Data {
  val fileNames = Seq(
    "link-enwiki-20180601-pages-articles1.xml-p10p30302",
    "link-enwiki-20180601-pages-articles2.xml-p30304p88444",
    "link-enwiki-20180601-pages-articles3.xml-p88445p200507",
    "link-enwiki-20180601-pages-articles4.xml-p200511p352689",
    "link-enwiki-20180601-pages-articles5.xml-p352690p565312",
    "link-enwiki-20180601-pages-articles6.xml-p565314p892912",
    "link-enwiki-20180601-pages-articles7.xml-p892914p1268691",
    "link-enwiki-20180601-pages-articles8.xml-p1268693p1791079",
    "link-enwiki-20180601-pages-articles9.xml-p1791081p2336422",
    "link-enwiki-20180601-pages-articles9.xml-p1791081p2336422"
  ).map("/test-data/enwiki-pages-articles/" ++ _)
}

object WebSmallData extends Data {
  val fileNames = Seq("/small-web.txt")
}

object WebData extends Data {
  val fileNames = Seq("/web-BerkStan.txt")
}

object PageRank {
    import org.apache.spark.rdd.RDD
    import org.apache.spark.HashPartitioner
    import org.apache.spark.RangePartitioner
    val nIterations = 10
    val OnlyLeft = true // Only give rank of nodes appearing on left side
    val IgnoreZeroIncoming = true // Only give ranks of nodes that have atleast one incoming Url

    if (OnlyLeft)
      println("INFO: Will only compute ranks of left side")

    if (IgnoreZeroIncoming)
          println("INFO: Will only compute ranks of nodes which had incoming urls")
	  
    val data = EnWikiData.rdd
    val cleandata = data.filter(!_.startsWith("#"))

    val edges = cleandata
        .map( line => line.split("\t", 2))
        .map(_.map(_.trim))
        .map(_.filter(_.nonEmpty))
        .filter(l => l.size == 2 && (!l(1).contains(":") || l(1).startsWith("category:")))
        .map(l => l(0) ->  l(1))
        .partitionBy(new HashPartitioner(150))

    val graph = edges.groupByKey()

    val initialRanks = graph.mapValues(_ => 1.0)

    def newRanks(
      graph: RDD[(String, Iterable[String])],
      prevRanks: RDD[(String, Double)]
    ): RDD[(String, Double)] = {
        val localIgnoreZeroIncoming = IgnoreZeroIncoming
        val contribReceived = graph.join(prevRanks).flatMap {
	  case (src, (dests, srcRank)) => {
	    val contrib = srcRank / dests.size
	    dests.map(d => d -> contrib) ++
	    (if (!localIgnoreZeroIncoming) Seq(src -> 0.0) else Seq())
	  }
	}

        val contribReceivedPerNode = contribReceived.reduceByKey((a, b) => a + b)
        contribReceivedPerNode.mapValues(contribRecv => 0.15 + 0.85 * contribRecv)
    }

    val finalAllRanks = (1 to nIterations).foldLeft(initialRanks) {
      case (prevRanks, _) => newRanks(graph, prevRanks)
    }
    val finalRanks =
      if (OnlyLeft) finalAllRanks.join(graph).map {case (n, (r, _)) => (n, r)}
      else finalAllRanks
}

import PageRank.{initialRanks, newRanks, edges, graph, finalRanks}
def show[T](rdd: org.apache.spark.rdd.RDD[T]): Unit = rdd.collect().foreach(println)
finalRanks.repartition(1).toDS.write.mode("overwrite").option("delimiter", "\t").csv("/finalRanks")