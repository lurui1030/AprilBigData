package com.marlabs.bigdata.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Source, Codec}

/**
  * Created by yuyu on 5/24/17.
  */
object TopRatedMovies {

    def parseRating(line : String) = {
        val columns = line.split("\\s+")
        val movieId = columns(1).toInt
        val rating = columns(2).toInt
        (movieId, (rating, 1))
    }

    def parseMovie(line : String) = {
        val columns = line.split("\\|")
        val movieId = columns(0).toInt
        val name = columns(1)
        (movieId, name)
    }

    def main (args : Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val sc = new SparkContext("local[*]", "TopRatedMovies")
        val ratingLines = sc.textFile("/Users/yuyu/Downloads/rating.txt")
        val movieLines = sc.textFile("/Users/yuyu/Downloads/movie.txt")

        val ratings = ratingLines.map(parseRating)
        val movies = movieLines.map(parseMovie)
        val totalRatings = ratings.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        val popularRating = totalRatings.filter( x => x._2._2 > 50)
        val avgRating = popularRating.mapValues(x => (x._1.toDouble / x._2))
        val descSortedByRating = avgRating.sortBy(_._2,false)
        val topTen = sc.parallelize(descSortedByRating.take(10))
        val descSortedMoviesWithName = topTen.leftOuterJoin(movies).sortBy(_._2._1, false)
        val result = descSortedMoviesWithName.collect()
        result.foreach(result => println(result._2._2.get, BigDecimal(result._2._1).setScale(1, BigDecimal.RoundingMode.HALF_UP)))

    }
}

