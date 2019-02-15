import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utility.Utility


object PageRankGraph {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("""Please pass two arguments for (1) Input File Directory (2) Output File Path. All files are on HDFS""")
      System.exit(0)
    }
    PageRank(args(0), args(1))
  }

  def PageRank(inputFileDir:String, outputFile: String) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val nIterations = 10
    val OnlyLeft = true // Only give rank of nodes appearing on left side
    val IgnoreZeroIncoming = true // Only give ranks of nodes that have atleast one incoming Url
    if (OnlyLeft)
      println("INFO: Will only compute ranks of left side")

    if (IgnoreZeroIncoming)
      println("INFO: Will only compute ranks of nodes which had incoming urls")

    val data = sc.textFile(inputFileDir)
    val cleanData = data.filter(!_.startsWith("#"))
      .map(x => x.toLowerCase()).filter { x =>
      val pair = x.trim().split("\\t+")
      pair.size == 2 && (!pair(0).contains(":") || pair(0).startsWith("category:") && (!pair(1).contains(":") || pair(1).startsWith("category:")))
    }

    val edges = cleanData
      .map(line => line.split("\\t+"))
      .map(_.map(_.trim))
      .map(_.filter(_.nonEmpty))
      .filter(_.length == 2)
      .map(_.map(_.toLowerCase()))
      .filter(_.forall(x => !x.contains(":") || x.startsWith("category:")))
      .map(l => l(0) -> l(1))
      .partitionBy(new HashPartitioner(150))

    val nNeighbours: RDD[(String, Int)] = edges
      .mapValues(_ => 1)
      .reduceByKey(_ + _)

    val initialRanks = edges.groupByKey().mapValues(_ => 1.0)

    def newRanks(edges: RDD[(String, String)], prevRanks: RDD[(String, Double)]): RDD[(String, Double)] = {
      val localIgnoreZeroIncoming = IgnoreZeroIncoming
      val contribReceived = edges
        .join(prevRanks)
        .join(nNeighbours)
        .flatMap {
          case (src, ((dest, srcRank), nNeighboursOfSrc)) =>
            Seq(dest -> srcRank / nNeighboursOfSrc) ++
              (if (!localIgnoreZeroIncoming) Seq(src -> 0.0) else Seq())
        }
      val contribReceivedPerNode = contribReceived.reduceByKey((a, b) => a + b)
      contribReceivedPerNode.mapValues {
        contribRecv => 0.15 + 0.85 * contribRecv
      }
    }

    val finalAllRanks = (1 to nIterations).foldLeft(initialRanks) {
      case (prevRanks, _) => newRanks(edges, prevRanks)
    }

    val finalRanks =
      if (OnlyLeft) finalAllRanks.join(initialRanks).map { case (n, (r, _)) => (n, r) }
      else finalAllRanks

    finalRanks.coalesce(1, true).saveAsTextFile(outputFile)
  }
}
