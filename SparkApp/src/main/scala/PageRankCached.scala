import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utility.Utility

object PageRankCached {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("""Please pass three arguments for (1) Input File Directory (2) Output File Path and (3) Number Of Partitions. All files are on HDFS""")
      System.exit(0)
    }
    PageRank(args(0), args(1), args(2).toInt)
  }


  def PageRank(inputFileDir:String, outputFile: String, partitions:Int) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val nIterations = 10
    val OnlyLeft = false // Only give rank of nodes appearing on left side
    val IgnoreZeroIncoming = true // Only give ranks of nodes that have atleast one incoming Url

    if (OnlyLeft)
      println("INFO: Will only compute ranks of left side")

    if (IgnoreZeroIncoming)
      println("INFO: Will only compute ranks of nodes which had incoming urls")

    val data = sc.textFile(inputFileDir)
    val cleanData = data.filter(!_.startsWith("#"))

    val edges = cleanData
      .map(line => line.split("\\t+"))
      .map(_.map(_.trim))
      .map(_.filter(_.nonEmpty))
      .filter(_.length == 2)
      .map(_.map(_.toLowerCase()))
      .filter(_.forall(x => !x.contains(":") || x.startsWith("category:")))
      .map(l => l(0) -> l(1))

    val graph = edges.groupByKey().partitionBy(new HashPartitioner(partitions)).cache()
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


