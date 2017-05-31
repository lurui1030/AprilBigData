package com.marlabs.bigdata


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

// Yu Chen

object MostPopMovie {
  
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
  def main(args: Array[String]) = {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
  
    val sc = new SparkContext("local[*]", "MostPopMovies")
    
    var nameDict = sc.broadcast(loadMovieName)
  
    val lines = sc.textFile("../homework/rating.txt")
  
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
  
    val moviesCounts = movies.reduceByKey( (x, y) => x + y )
  
    val exchanged = moviesCounts.map( x => ( x._2, x._1) )
  
    val sortedMovies = exchanged.sortByKey()
    
    val sortedMoviesWithNames = sortedMovies.map( x => (nameDict.value(x._2), x._1))
  
    val results = sortedMoviesWithNames.collect()
  
    results.foreach(println)
  }          
}