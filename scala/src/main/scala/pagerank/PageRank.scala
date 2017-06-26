package pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object PageRank {

  val INPUT_LOCATION = "hdfs:///user/maria_dev/PageRank/source.txt"
  val OUTPUT_LOCATION = "hdfs:///user/maria_dev/PageRank/output"
  val ITERATIONS = 1

  def main() {

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    //Load dataset into a RDD
    val data = sc.textFile(INPUT_LOCATION)

    //Filter out comments, Split the row into an Array, Represent each row as a tuple
    val links = data.filter(!_.contains("#")).map(_.split("\t")).map(x => (x(0), x(1))).groupByKey.cache()

    //Initialize a ranks RDD with all ranks = 1
    var ranks = links.mapValues(value => 1.0)

    for (i <- 1 to ITERATIONS) {

      //Join the links and ranks RDDs
      val linksWithRank = links.join(ranks)

      //Each node transfers its rank equally to its neighbors
      //(892689,(CompactBuffer(51257, 345031, 630192, 827675),1.0)
      val contribs = linksWithRank.values.flatMap({case (nodes, rank) =>
        val size = nodes.size
        //nodes.map(x => (x, rank/size))
        for(x <- nodes) yield {(x, rank/size)}
      })

      //Sum up all values of the same node and apply the damping factor
      ranks = contribs.reduceByKey(_ + _).mapValues(0.2 + 0.8 * _)
    }

    //Save the RDD to HDFS
    ranks.saveAsTextFile(OUTPUT_LOCATION)
  }
}