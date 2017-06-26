package streaming1

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Before running this program, you MUST start a stream at your localhost
  * using the netcat utility. Below is the command:
  * nc -l 9000
  */

object Streaming {

  val BACKUP_LOCATION = "hdfs:///user/cloudera/streaming/backup"
  val HOST = "localhost"
  val PORT = 9000

  def main() {

    val conf = new SparkConf().setAppName("page rank").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //keep ERROR logs ONLY
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    //Set up a StreamingContext, a entry point for Spark Streaming.
    // sparkcontext, batch interval
    val ssc = new StreamingContext(sc, Seconds(1))

    //Set up a location where a backup of data is saved
    ssc.checkpoint(BACKUP_LOCATION)

    //Create a DStream with one secend as batch interval
    //The DStream reads text data which arrives at a specific host and port
    val lines = ssc.socketTextStream(HOST, PORT)

    //Apply transformation and action on the DStream
    val counts = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    counts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
