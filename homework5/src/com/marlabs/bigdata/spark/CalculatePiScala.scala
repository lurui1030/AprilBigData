package com.marlabs.bigdata.spark

import scala.util.Random

/**
  * Created by yuyu on 5/24/17.
  */
object CalculatePiScala {
    def main (args : Array[String]): Unit = {
        val totalNum = args(0).toInt;
        var numInCircle = 0
        for (num <- 1 to totalNum) {
            val abscissa = Random.nextDouble()
            val ordinate = Random.nextDouble()
            if (abscissa * abscissa + ordinate * ordinate <= 1) {
                numInCircle += 1
            }
        }
        val pi = 4 * numInCircle.toDouble/totalNum
        println(f"$pi%1.5f")
    }
}
