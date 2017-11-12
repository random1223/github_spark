import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.math.min

object TemperatureCount {



  def convertStationAndTemperature(s:String) = {

    val splitAry = s.split(",");
    val temp = splitAry(3).toInt;
    (splitAry(0), temp)

  }

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("1800.csv")

    val linesForTMIN = lines.filter(x => {x.contains("TMIN")})
    val mapStationAndTemp = linesForTMIN.map(convertStationAndTemperature)
//    {A, -100}, {B, -200}

    val reducedStationAndTemp = mapStationAndTemp.reduceByKey((x,y) => {min(x,y)} )
//    mapStationAndTemp.reduceByKey(x => {x._1,})
//    mapStationAndTemp.mapValues(x => {  })
    val mapResult = reducedStationAndTemp.collect()
//    mapResult.sorted.foreach(println)

    for (map <- mapResult.sorted){
      val station = map._1
      val temp = map._2
      println(s"$station, $temp ")
    }




  }

}
