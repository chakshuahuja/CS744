import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import utility.Utility

object PageRankCached {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("""Please pass argument for (1) Output File Path. All files are on HDFS""")
      System.exit(0)
    }
    PageRank(args(0))
  }


  def PageRank(outputFile: String) {
    val nIterations = 10
    val OnlyLeft = true // Only give rank of nodes appearing on left side
    val IgnoreZeroIncoming = true // Only give ranks of nodes that have atleast one incoming Url

    if (OnlyLeft)
      println("INFO: Will only compute ranks of left side")

    if (IgnoreZeroIncoming)
      println("INFO: Will only compute ranks of nodes which had incoming urls")

    val data = (new Utility).EnWikiData.rdd
    val cleanData = data.filter(!_.startsWith("#"))
      .map(x => x.toLowerCase()).filter { x =>
      val pair = x.trim().split("\\t+")
      pair.size == 2 && (!pair.last.contains(":") || pair.last.startsWith("category:"))
    }

    val edges = cleanData
      .map(line => line.split("\\t+"))
      .map(_.map(_.trim))
      .map(_.filter(_.nonEmpty))
      .filter(_.length == 2)
      .map(l => l(0) -> l(1))
      .partitionBy(new HashPartitioner(150))

    val graph = edges.groupByKey()
    val initialRanks = graph.mapValues(_ => 1.0)

    def newRanks(graph: RDD[(String, Iterable[String])], prevRanks: RDD[(String, Double)]): RDD[(String, Double)] = {
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
      if (OnlyLeft) finalAllRanks.join(graph).map { case (n, (r, _)) => (n, r) }
      else finalAllRanks

    finalRanks.coalesce(1, true).saveAsTextFile(outputFile)
  }

}


