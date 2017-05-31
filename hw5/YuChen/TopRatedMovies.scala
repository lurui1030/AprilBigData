package com.marlabs.bigdata

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

// Yu Chen

object TopRatedMovies {
  
  def loadMovieName() : Map[Int, String] = {
    
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movieNames:Map[Int, String] = Map()
    
    val lines = Source.fromFile("../homework/movie.txt").getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    
    return movieNames
  }
  
  def parseRatings(line : String) = {
    val columns = line.split("\t")
    val movieId = columns(1).toInt
    val rating = columns(2).toInt
    (movieId, (rating, 1)) 
  }
  
  def main(args : Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "TopRatedMovies")
    
    var nameDict = sc.broadcast(loadMovieName)
    
    val ratinglines = sc.textFile("../homework/rating.txt")
    
    val movies = ratinglines.map(parseRatings)
    
    
    val totalRatings = movies.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2 ))
    
    val popRatings = totalRatings.filter( x => x._2._2 > 30 )
    
    val avgRatings = popRatings.mapValues( x => x._1.toDouble / x._2 )
    
    val exchanged = popRatings.map( x => (x._2, x._1) )
    
    val sortedRating = exchanged.sortByKey(false).take(10)
    
    val sortedRatingsWithName = sortedRating.map( x => (nameDict.value(x._2), x._1))
    
    sortedRatingsWithName.foreach(println)
     
  }
}