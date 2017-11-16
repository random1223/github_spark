import org.apache.spark.sql.SparkSession

object DataSetsMy {


  final case class Friend(id: Int, name: String, age : Int, friends: Int)

  def convertMovie(line:String): Friend ={

    val lineSplit = line.split(",")
    return new Friend(lineSplit(0).toInt, lineSplit(1), lineSplit(2).toInt, lineSplit(3).toInt)

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Read in each rating line and extract the movie ID; construct an RDD of Movie objects.
    val lines = spark.sparkContext.textFile("fakefriends.csv").map(convertMovie)

    // Convert to a DataSet
    import spark.implicits._
    val moviesDS = lines.toDS()

    val sumFriendsByAge = moviesDS.groupBy("age").sum("friends").orderBy("sum(friends)").cache()
    sumFriendsByAge.show() //print out items
    sumFriendsByAge.take(100) //get 100 items
//    println(sumFriendsByAge)


  }

}
