import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkProcess_DF_RDD {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFrameToRDD")
      .master("local[*]") // You can specify your master URL here
      .getOrCreate()

    val inputFilePath = "/Users/chinmaykalyan/Desktop/50_Startups.csv"
    val df: DataFrame = spark.read
      .option("header", "true") // If the file has a header
      .csv(inputFilePath)

    val rdd = df.rdd

    val first10Elements = rdd.take(10)
    first10Elements.foreach(println)

    rdd.saveAsTextFile("/Users/chinmaykalyan/Desktop/Data/outputdftordd.txt")


    // Perform RDD operations or transformations as needed

    spark.stop()
  }
}
