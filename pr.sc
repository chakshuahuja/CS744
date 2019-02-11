val lines = sc.textFile("hdfs://128.104.223.156:9000/web-BerkStan.txt")
val mydata = lines.mapPartitionsWithIndex{case(i,iter) => if (i<4) iter.drop(4) else iter}
val links = mydata.map( x => x.split("\\t"))
val mappedlinks = links.map(x => (x(0), x(1))).persist()
mappedlinks.take(10).foreach(println)

var rankswithsize = sc.parallelize(mappedlinks.countByKey().toSeq)
rankswithsize.persist()
var ranksserial = rankswithsize.map{ case(key, value) => (key, (1.0, value)) }

for ( i<- 1 to 10) {

  var contribs = mappedlinks.join(ranksserial).map{ case(k, (dest, (r,size) )) => (dest, r.toDouble /size.toDouble) }
  var ranks = contribs.reduceByKey( (x,y) => x+y).mapValues(sum => 0.15 + 0.85*sum)
  ranksserial = ranks.join(rankswithsize)

}
var finalrank = ranksserial.collect()
var outputranks = sc.parallelize(finalrank.toSeq)
outputranks.map{ case(url, (rank, size) ) => (url +" "+rank) }.coalesce(1,true).saveAsTextFile("/users/Vibhor/outputs/pr_berk_results.csv")
