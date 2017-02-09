import org.apache.spark.{SparkConf, SparkContext}

//EXERCISE 3: Find the detail description of "972889".
object Detail {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val conf = new SparkConf().setAppName("Load from hdfs")
    val retail_data = sc.textFile("./data/retail_data")

    //it will give the full description of the above product id.
    val filter_retail_data = retail_data.filter(line => line.contains("972889"))

    //to display the ans.
    filter_retail_data.collect()
  }
}
