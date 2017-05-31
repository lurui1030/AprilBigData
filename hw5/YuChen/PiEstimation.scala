package com.marlabs.bigdata

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

// Yu Chen


object PiEstimation {
  def main(args:Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PiEstimation")
    
    val count = sc.parallelize(1 to 10000).filter { _ =>
    val x = math.random
    val y = math.random
    x*x + y*y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / 10000}")
  }
}