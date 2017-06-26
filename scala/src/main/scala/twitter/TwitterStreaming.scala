package twitter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

object TwitterStreaming {

  val BACKUP_LOCATION = "hdfs:///user/maria_dev/Streaming/Backup"

  def main() {

    System.setProperty("twitter4j.oauth.consumerKey",
      "UakXL6SBaKI4hKgSeBeEWvr0B");
    System.setProperty("twitter4j.oauth.consumerSecret",
      "CWzQBzsH4lyufyBZMulxRObvuadSV8j2FLwJ0LnViVo4fbUA6U");
    System.setProperty("twitter4j.oauth.accessToken",
      "2583801230-8dU3tDEvZ0gFAB5aszcUQJZRyJD8t6cyJtbEaeW");
    System.setProperty("twitter4j.oauth.accessTokenSecret",
      "4LgY8nXrkfRrXo3QcTJBZcZdEexApp4VAG6RYjQev7GjG");

    val sc = new SparkContext(new SparkConf().setAppName("twitter").setMaster("local"))
    sc.setLogLevel("ERROR")
    //Set up a StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))

    //Set up a location where a backup of data is saved
    ssc.checkpoint(BACKUP_LOCATION)
    //Create a DStream with one secend as batch interval
    val tweets = TwitterUtils.createStream(ssc, None)


    //val statuses = tweets.map(status => status.getText())
    val hashtags = tweets.flatMap(status => status.getText().split(" "))
      .filter(_.startsWith("#"))
      .map(x => (x, 1))
      .reduceByKeyAndWindow(
        _+_,
        _-_,
        Seconds(10),
        Seconds(30)
      )
      .filter(_._2 != 0)
      .transform(x => {
        x.sortBy(_._2)
      })
    //statuses.print()
    hashtags.transform(x => {
      val list = x.first()
      x.filter(_ == list)
    }).saveAsTextFiles("hdfs:///user/maria_dev/Streaming/Result")

    ssc.start()
    ssc.awaitTermination()
  }

}
