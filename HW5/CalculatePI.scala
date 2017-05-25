package com.bigdata.homework

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by junchengma on 5/25/17.
  */
object CalculatePI {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CalculatePI")
    val count = sc.parallelize(1 to 20000000).filter { _ =>

      val x = math.random
      val y = math.random

      x * x + y * y <= 1
    }.count()

    println(s"Pi is roughly: ${4.0 * count / 20000000}")
  }


}
