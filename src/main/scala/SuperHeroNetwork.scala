import org.apache.log4j._
import org.apache.spark._

import scala.io.Source

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object SuperHeroNetwork {

  def dataProcessing(line: String) = {
    val splitAry = line.split(",");
    val age = splitAry(2).toLong;
    val friends = splitAry(3).toLong;
    val name:String = splitAry(1);

    (age, friends)
  }

  def finalMap() : Map[Int, String] = {
    val lines = Source.fromFile("Marvel-names.txt").getLines()
    var map:Map[Int, String] = Map()
    for(line<-lines){
      val ary = line.split(" ")
      map += (ary(0).toInt -> ary(1))
    }
    return map
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("Marvel-graph.txt")

    var nameMap = sc.broadcast(finalMap)

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val allLines = lines.flatMap(x => x.split("\\s"))

    val distinctWords = allLines.map(x=>(x, 1)).reduceByKey((x,y) => x+y).sortBy(x=>x._2)

    val result = distinctWords.collect()

    for(obj <- result){

      val heroId = obj._1
      val count = obj._2
      val heroName = nameMap.value(heroId.toInt)
      println(s"$heroId, $count, $heroName")
    }

//    val count = allLines.countByValue()

//    count.foreach(distinctWords)

//    count.foreach(println)
//    result.foreach(v => {println})

//
//    val filteredRatings = ratings.filter((str : String) => {!str.contains("0")})
//
//    // Count up how many times each value (rating) occurs
//    val results = filteredRatings.countByValue()
//
//
//    // Sort the resulting map of (rating, count) tuples
//    val sortedResults = results.toSeq.sortBy(_._1)
//
//    // Print each result on its own line.
//    sortedResults.foreach(println)
  }
}
