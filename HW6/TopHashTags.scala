package com.bigdata.courseAndHomework

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

/**
  * Created by junchengma on 6/6/17.
  */
object TopHashTags {

  val BACKUP_LOCATION = "hdfs:///user/maria_dev/Streaming/Backup"
  val SAVE_LOCATION = "hdfs:///user/maria_dev/Streaming/Twitter/Tweet_"


  def setupTwitter() : Unit = {
    System.setProperty("twitter4j.oauth.consumerKey", "CkPMkiirXrLaBitghGFRDYDYG")
    System.setProperty("twitter4j.oauth.consumerSecret", "qTSr7CPiWZPs7I4fljtWe4rs629elJiU1uW7jsBqA5jT5h1AY5")
    System.setProperty("twitter4j.oauth.accessToken", "3109132660-tSEEcwt8bR2aF7rbPR0J3ZTttd8KFysxv3Q5zTT")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "mjbZPjOz5R4P8z2rG1rGHwx7Z4rf2Y7YLl3umDsZAn1g7")
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    setupTwitter()

   // val ssc = new StreamingContext("local[*]", "PopularHashTags", Seconds(1))
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint(BACKUP_LOCATION)

    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText())
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1))
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    sortedResults.print
    sortedResults.saveAsTextFiles(SAVE_LOCATION)

    ssc.start()
    ssc.awaitTermination()
  }

}
