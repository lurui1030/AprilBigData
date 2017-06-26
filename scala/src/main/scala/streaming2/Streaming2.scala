package streaming2

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Before running this program, you MUST start a stream at your localhost
  * using the netcat utility. Below is the command:
  * nc -l 9000
  */

object Streaming {

  val BACKUP_LOCATION = "hdfs:///user/maria_dev/Streaming/Backup"
  val HOST = "localhost"
  val PORT = 9000

  def main() {

    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming 2")
    val sc = new SparkContext(conf)

    //keep ERROR logs ONLY
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    //Set up a StreamingContext
    val ssc = new StreamingContext(sc, Seconds(1))

    //Set up a location where a backup of data is saved
    ssc.checkpoint(BACKUP_LOCATION)

    //Create a DStream with one secend as batch interval
    //The DStream reads text data which arrives at a specific host and port
    val lines = ssc.socketTextStream(HOST, PORT)


    /**
      * Apply transformation and action on the DStream
      * Stateful transformation using sliding windows
      * Three intervals: windows, sliding, batch
      */
    val counts = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow(
      {(x,y) => (x+y)},
      {(x,y) => (x-y)},
      Seconds(30), Seconds(10)
    )

    counts.print()

    ssc.start()
    ssc.awaitTermination()

  }



}

