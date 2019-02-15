package utility

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.net._

import scala.collection.mutable.ListBuffer

class Utility {
  abstract class Data {

    def fileNames: Seq[String]
    val spark = SparkSession.builder().appName("SampleDataSortApp").getOrCreate
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


  def InputFileDirInHDFS(inputFileDir:String):Boolean = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://"+InetAddress.getLocalHost.getHostAddress+":9000"),new Configuration())
    val dirPath = new Path(inputFileDir)
    var temp = new ListBuffer[String]()
    if(fs.exists(dirPath) && fs.isDirectory(dirPath)) {
      true
    } else {
      false
    }
  }

  def InputFileInHDFS(inputFile:String):Boolean = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://"+InetAddress.getLocalHost.getHostAddress+":9000"),new Configuration())
    val filePath = new Path(inputFile)
    var temp = new ListBuffer[String]()
    if(fs.exists(filePath) && fs.isFile(filePath)) {
      true
    } else {
      false
    }
  }
}
