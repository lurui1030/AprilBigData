package com.marlabs.bigdata.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.util.Random

/**
  * Created by yuyu on 5/23/17.
  */
object CalculatePi {

    def main (args : Array[String]): Unit = {
        val totalNum = args(0).toInt
        val pointList = generateRandomPoints(totalNum)
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]", "CalculatePi")
        val totalPoints = sc.parallelize(pointList)
        val pointsInCircle = totalPoints.filter(points => points._1 * points._1 + points._2 * points._2 <= 1)
        val count = pointsInCircle.count
        val pi = 4 * count.toDouble/totalNum
        println(f"$pi%1.5f")
    }
    //abscissa, ordinate
    def generateRandomPoints(num : Int) : List[(Double, Double)] = {
        var pointList = List.empty[(Double, Double)]
        for (i <- 1 to num) {
            pointList = pointList :+ (Random.nextDouble(), Random.nextDouble())
        }
        return pointList
    }
}
