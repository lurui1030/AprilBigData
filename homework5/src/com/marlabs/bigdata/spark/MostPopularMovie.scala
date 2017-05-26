package com.marlabs.bigdata.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Source, Codec}

/**rror
  * Created by yuyu on 5/24/17.
  */
object MostPopularMovie {

    def parseRating(line : String) = {
        val columns = line.split("\\s+")
        val movieId = columns(1).toInt
        (movieId, 1)
    }

    def loadMovie() : Map[Int, String]= {

        implicit val codec = Codec("UTF-8")
        codec.onMalformedInput(CodingErrorAction.REPLACE)
        codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

        var idName:Map[Int, String] = Map()
        val lines = Source.fromFile("/Users/yuyu/Downloads/movie.txt").getLines()
        for (line <- lines) {
            var fields = line.split("\\|")
            if (fields.length > 1) {
                idName += (fields(0).toInt -> fields(1))
            }
        }
        return idName
    }

    def main(args : Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]", "MostPopularMovie")
        val ratingLines = sc.textFile("/Users/yuyu/Downloads/rating.txt")
        val nameDict = sc.broadcast(loadMovie)
        val ratings = ratingLines.map(parseRating)
        val ratingNumPerMovie = ratings.reduceByKey((num1, num2) => (num1 + num2))
        val flipped = ratingNumPerMovie.map(x => (x._2, x._1))
        val descSortedByRatingNum = flipped.sortByKey(false)
        val descSortedMoviesWithName = descSortedByRatingNum.map( x => (nameDict.value(x._2), x._1))
        val result = descSortedMoviesWithName.take(10)
        result.foreach(println)
    }
}
