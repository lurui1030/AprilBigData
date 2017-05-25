package com.bigdata.homework

import java.nio.charset.CodingErrorAction
import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

/**
  * Created by junchengma on 5/25/17.
  */
object Top10RatedMovies {

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

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Top10RatedMovies")
    var nameDict = sc.broadcast(loadMovieNames)
    val lines = sc.textFile("/Users/junchengma/Downloads/spark/rating.txt")

    def parseRating(line : String) = {
      val fields = line.split("\t")
      val movieId = fields(1).toInt
      val rating = fields(2).toFloat
      (movieId, rating)
    }

    val pairs = lines.map(x => parseRating(x))
    val ratings = pairs.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val filtered = ratings.filter(x => x._2._2 > 100)
    val avgRating = filtered.mapValues(x => x._1 / x._2)
    val flipped = avgRating.map(x => (x._2, x._1))
    val sortedRating = flipped.sortByKey(false).take(10)
    val topRatedMovies = sortedRating.map(x => (nameDict.value(x._2), x._1))

    topRatedMovies.foreach(println)
  }

}
