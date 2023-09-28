
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StructField, StringType}
object SparkProcess_RDD_DF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDDToDataFrame")
      .master("local[*]") // You can specify your master URL here
      .getOrCreate()

    val rddData = Seq("John", "Jane", "Bob", "Alice")
    val rdd: RDD[String] = spark.sparkContext.parallelize(rddData)

    val schema = StructType(Seq(StructField("name", StringType, true)))

    val df = spark.createDataFrame(rdd.map(Row(_)), schema)

    df.write
      .format("csv")
      .option("header", "true")
      .save("/Users/chinmaykalyan/Desktop/Data/output")

    spark.stop()
  }
}
