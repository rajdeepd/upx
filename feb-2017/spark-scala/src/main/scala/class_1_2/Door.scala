// EXERCISE 2: Find Records having different Door types
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Door {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val conf = new SparkConf().setAppName("Load from hdfs")
    val retail_data = sc.textFile("hdfs:///user/support1161/retail_data")
    val filter_retail_data = retail_data.filter(line => line.contains("Door"))
    filter_retail_data.collect()
  }
}