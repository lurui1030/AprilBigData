import scala.math.random
import org.apache.spark._

val n = 1000000
val xs = 1 until n 
val rdd = sc.parallelize(xs, 10)
val sample = rdd.map { i =>
  val x = random * 2 - 1
  val y = random * 2 - 1
  (x, y)
}
 
val inside = sample.filter { case (x, y) => (x * x + y * y <= 1) }

val count = inside.count()
 
println("Pi is roughly " + 4.0 * count / n)
