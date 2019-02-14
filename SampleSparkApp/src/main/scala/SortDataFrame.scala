import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.asc

object SortDataFrame {
  def main(args: Array[String]): Unit = {
    if (args.length<2) {
      println("""Please pass two arguments for (1) Input File Path and (2) Output File Path. All files are on HDFS""")
      System.exit(0)
    }
    sortDataSet(args(0), args(1))
  }

  def sortDataSet(inputFile:String, outputFile:String): Unit = {
    val spark = SparkSession.builder().appName("SampleDataSortApp").getOrCreate
    val df = spark.read.format("csv").option("header", "true").load(inputFile)
    val sortedDF = df.sort(asc("cca2"), asc("timestamp"))
    sortedDF.coalesce(1).write.option("header", "true").csv(outputFile)
    println(s"Output of sorted DataSet can be found on HDFS here: $outputFile")
  }
}
