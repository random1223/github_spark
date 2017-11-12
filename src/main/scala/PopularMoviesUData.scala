import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Source

object PopularMoviesUData {



  def converData(s:String) = {
    val splitAry = s.split("\\t");
    val movieId = splitAry(1);
    val rating = splitAry(2).toInt
    (movieId, rating)
  }

  var movieNameAndIdMap:Map[Int, String] = Map()
  def converMovieName() : Map[Int,String] = {

    val allLines = Source.fromFile("u.item").getLines()
    for(line <- allLines){
      val splitAry = line.split('|');
      val movieId = splitAry(0).toInt;
      val movieName = splitAry(1)
//      println(movieId)
      movieNameAndIdMap += (movieId -> movieName)
//      map.+(movieId, movieName)
    }
    return movieNameAndIdMap
  }



  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")


    val movieNameLines = sc.textFile("u.item")

    var idNameMap = sc.broadcast(converMovieName)

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("u.data")

    val mapStationAndTemp = lines.map(converData)

    val reducedStationAndTemp = mapStationAndTemp.mapValues(x=>(x,1)).reduceByKey((x,y) => {(x._1+y._1, x._2+y._2)} )
    val avgRatingAndCount = reducedStationAndTemp.mapValues((x) => (1.0*x._1/x._2, x._2))
    val mapResult = avgRatingAndCount.map((x)=>(x._2._2, x._1, x._2._1)).sortBy(x=>x._3).collect()
//    mapResult.sorted.foreach(println)

    for (map <- mapResult.sorted){
      val movieId = map._1
      val count = map._2
      val sumRating = map._3
      val name1 = movieNameAndIdMap(movieId)
      val name2 = idNameMap.value(movieId)
      println(s"$movieId, $count $sumRating $name1")
    }




  }

}
