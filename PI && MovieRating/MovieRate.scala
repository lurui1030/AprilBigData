package com.marlabs.bigdata.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object MovieRate {
  
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  
  def parseRating(line:String)= {
    val fields = line.split("\\t")
    val movie_id = fields(1).toInt
    val rate = fields(2).toDouble
    
    (movie_id,rate)
  }
  
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","Movie Rating")
    
    //load rating file
    val ratingLines = sc.textFile("../u.data")
    
    // get movieID and rating
    val ratings = ratingLines.map(parseRating)
    
    // get moive name
    val movieDict = loadMovieNames()
    
    //get each movie's rating sum and rating numbers
    val totalRating = ratings.mapValues(x => (x,1.0d)).reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).filter(x => x._2._2>100)
    
    // calculate the average rating, sort by average rating
    val  avgRating = totalRating.mapValues(x => x._1/x._2.toDouble).sortBy(_._2,false)
    
    //get the top 10
    val top10 = avgRating.take(10).map(x => (movieDict(x._1),x._2))
    
    top10.foreach(println)
        
    // build the (movieId,ratingNum) pari
    val popularMovies = totalRating.map(x => (x._1,x._2._2)).sortBy(_._2,false)
    
    //get the most mopoular one
    val mostPopular = popularMovies.take(1).head
    
    println("The most popular movie is: " + movieDict(mostPopular._1))
    
  }
  
}
