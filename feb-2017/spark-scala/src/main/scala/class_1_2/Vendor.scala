import org.apache.spark.{SparkConf, SparkContext}

//EXERCISE 5: Find which vendor has high availability of "Purple Swirl Soap and Lotion Pump product


object Vendor {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "")
    val conf = new SparkConf().setAppName("Load from hdfs")

    val retail_data = sc.textFile("./data/retail_data")


    val filter_retail_data = retail_data.map(x => x.split("\\|"))
    //Keeping only required records having Purple Swirl Soap and Lotion Pump as product
    val req_filter_retail_data = filter_retail_data.filter(x => x.contains("Purple Swirl Soap and Lotion Pump"))
    val gen_split_retail_data = req_filter_retail_data.map(line => (line(1).toInt,(line(0),line(3)))).sortByKey(false)
    println(gen_split_retail_data.take(1)(0))

    sc.stop()
  }
}
