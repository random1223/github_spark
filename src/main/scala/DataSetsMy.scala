import org.apache.spark.sql.SparkSession

object DataSetsMy {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()


  }

}
