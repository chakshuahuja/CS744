import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import scala.collection.mutable.ListBuffer

import java.net.URI
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._


object SparkDataSet {

  def main(args: Array[String]): Unit = {
//    if (args.length<2) {
//      println("Please pass two arguments for (1) Input File Path and (2) Output File Path")
//      System.exit(0)
//    }
    //sortDataSet(args(0), args(1))
    sortDataSet("src/main/resources/export.csv","src/main/resources/rddSort1.txt")
  }

  def sortDataSet(inputFile:String, outputFile:String): Unit = {
    val conf = new SparkConf().setAppName("SampleDataSortApp").setMaster("local")
    val sc = new SparkContext(conf)

    //Using DataFrames
    val spark = SparkSession.builder().appName("SampleDataSortApp").getOrCreate
    val df = spark.read.format("csv").option("header", "true").load(inputFile)
    val sortedDF = df.sort(asc("cca2"), asc("timestamp"))
    sortedDF.coalesce(1).write.option("header", "true").csv(outputFile)
    println(s"Output of DataSet sorted can be found here: $outputFile")

    //Using RDD
//    val csvData = sc.textFile(inputFile)
//    val data = csvData.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
//    val sortedRDD = data.sortBy(x => (x.split(",")(2), x.split(",")(14)))
//    sortedRDD.collect.foreach(println)
//    sortedRDD.coalesce(1, true).saveAsTextFile(outputFile)
}


  def pageRank(inputFileDir:String, outputFile:String): Unit = {
    val conf = new SparkConf().setAppName("SampleDataSortApp").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("SampleDataSortApp").getOrCreate
    //pass hdfs folder location
    //val wikiData = sc.wholeTextFiles("hdfs://128.104.223.156:9000/wikiData/*") //List[List[String]]

    //Seach in local folder
    var wikiFiles = getListOfFilesFromLocal(inputFileDir) //getListOfFiles("/test-data/enwiki-pages-articles")
    if (wikiFiles.isEmpty) {
      //Search in HDFS folder
      wikiFiles = getListOfFilesFromHDFS(inputFileDir)
    }
    if (wikiFiles.isEmpty) {
      println("No file found to read from specific directory")
      System.exit(0)
    }
    val wikiData = wikiFiles.map(spark.read.textFile(_).rdd).reduce((rdd1, rdd2) => rdd1.union(rdd2))

    //Filter dataset
    val filteredData = wikiData.map(x => x.toLowerCase()).filter{x =>
      val pair = x.trim().split("\\t+|\\s+")
      pair.size==2 && (!pair.last.contains(":") || pair.last.startsWith("category:"))
    }.cache()

    val links = filteredData.map(s => s.split("\\t+|\\s+")).filter(x => x.nonEmpty && x.size==2).map(parts => (parts(0), parts(1))).distinct().groupByKey().cache()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to 10) {
      val contributions = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).mapValues(sum => 0.15 + 0.85 * sum)
    }

    ranks.coalesce(1, true).saveAsTextFile(outputFile)
    //val finalRank = ranks.collect()
    //finalRank.take(10).foreach(println)
  }


  def getListOfFilesFromLocal(directory: String):List[String] = {
    val dir = new File(directory)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList.map(_.toString)
    } else {
      List[String]()
    }
  }

  def getListOfFilesFromHDFS(inputFileDir:String):List[String] = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://128.104.223.156:9000"),new Configuration())
    val dirPath = new Path(inputFileDir)
    var temp = new ListBuffer[String]()
    if(fs.exists(dirPath) && fs.isDirectory(dirPath)) {
      val fileStatusList = fs.listStatus(dirPath)
      fileStatusList.foreach(f => temp += f.getPath.toString)
    }
    temp.toList
  }

}
