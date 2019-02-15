import org.apache.spark.{SparkConf, SparkContext}

object SortRDD {
  def main(args: Array[String]): Unit = {
    if (args.length<2) {
      println("""Please pass two arguments for (1) Input File Path and (2) Output File Path. All files are on HDFS""")
      System.exit(0)
    }
    sortDataSet(args(0), args(1))
  }

  def sortDataSet(inputFile:String, outputFile:String): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val csvData = sc.textFile(inputFile)
    val data = csvData.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val sortedRDD = data.sortBy(x => (x.split(",")(2), x.split(",")(14)))
    sortedRDD.coalesce(1, true).saveAsTextFile(outputFile)
    println(s"Output of sorted DataSet can be found on HDFS here: $outputFile")
  }
}