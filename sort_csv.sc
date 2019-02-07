// Using RDDs API

val myRDD = sc.textFile("/export.csv")
val dataRDD = myRDD.mapPartitionsWithIndex {case (i, iter) => if (i ==0) iter.drop(1) else iter }
val sortedRDD = dataRDD.sortBy(x => (x.split(",")(2), x.split(",")(14)))
sortedRDD.collect.foreach(println)
sortedRDD.coalesce(1, true).saveAsTextFile("/rdsortedinfile.csv")

