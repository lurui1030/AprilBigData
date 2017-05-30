package com.marlabs.bigdata.spark

import scala.util.Random 

object PICalculate {
  def inCircle(x:Double,y:Double):Boolean ={
     (x*x + y*y <1)
  }

  def getInCircle(range: Int):Int = {
    val r = scala.util.Random
    var counter = 0
    var x = 0.0d
    var y = 0.0d
    for(a <- 0 to range){
      x = r.nextDouble()*2-1
      y = r.nextDouble()*2-1
      if(inCircle(x,y)){
        counter+=1
      }
    }
  }
  
  def main(args:Array[String]){
    
    val range = 500000
    
    val counter = getInCircle(range)
    val result:Double = (counter.toDouble/range.toDouble)*4
    
    println(result)
    
    
  }
}