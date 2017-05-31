def parse(row: String)={
  val fields = row.split("\t")
  val movie_id = fields(1).toLong
  val rating = fields(2).toInt
  (movie_id, rating)
}

val data = sc.textFile("hdfs:///user/maria_dev/Spark/rating.txt")

val parsed = data.map(x => parse(x))

val SortPopular = parsed.map(x => x._1)

val totalRating = parsed.mapValues(x => (x, 1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))

val AverageRating = totalRating.mapValues(x => x._1.toDouble/x._2).sortBy(_._2,false)

val TopTen = AverageRating.take(10).map(x => x._1)

val MostPopular = SortPopular.countByValue().toList.sortWith(_._2 > _._2).take(1)

println("Id of the most popular movie is " + MostPopular(0)._1)

println("Top 10 Rated Moive Id are: ")

TopTen.foreach(i => print(i + " "))

