package com.bigdata.homework

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

/**
  * Created by junchengma on 5/25/17.
  */
object MostPopularMovie {

  def loadMovieNames() : Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("/Users/junchengma/Downloads/spark/movie.txt").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularMovie")
    var nameDict = sc.broadcast(loadMovieNames)
    val lines = sc.textFile("/Users/junchengma/Downloads/spark/rating.txt")
    val ratings = lines.map(x => (x.split("\t")(1).toInt, 1))
    val ratingCounts = ratings.reduceByKey((x, y) => x + y)
    val flipped = ratingCounts.map(x => (x._2, x._1))
    val sortedRatings = flipped.sortByKey(false).take(1)
    val mostPopularMovie = sortedRatings.map(x => (nameDict.value(x._2), x._1))
    mostPopularMovie.foreach(println)

  }

}
