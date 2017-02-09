package class_1_2

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Cato {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val conf = new SparkConf().setAppName("Load from hdfs")

    val nyse_data = sc.textFile(".//nyse")
    //search records having CATO in it
    val result = nyse_data.filter(line => line.contains("CATO"))
    //Displays the result
    result.collect()
  }
}
