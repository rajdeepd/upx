//EXERCISE 1: Load RETAIL File from HDFS
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object retail {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val conf = new SparkConf().setAppName("Load from hdfs")
    val retail_data = sc.textFile("./data/retail_data")
    println(retail_data.first())
  }
}
