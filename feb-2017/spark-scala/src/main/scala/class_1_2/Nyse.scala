import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Nyse {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val conf = new SparkConf().setAppName("Load from hdfs")
    val nyse_data = sc.textFile("./data/nyse")
    println(nyse_data.first())
  }
}
