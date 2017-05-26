package com.marlabs.bigdata.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.util.Random

/**
  * Created by yuyu on 5/25/17.
  */
object CalcultatePiTwo {

    def main(args : Array[String]): Unit = {
        val totalNum = args(0).toInt
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]", "CalculatePiTwo")
        val pointsInCircle = sc.parallelize(1 to totalNum).filter( _ => {
        val abscissa = Random.nextDouble()
        val ordinate = Random.nextDouble()
        abscissa * abscissa + ordinate * ordinate <= 1})
        val count = pointsInCircle.count

        println(4 * count.toDouble / totalNum)
    }

}
