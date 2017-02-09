import org.apache.spark.SparkContext

object AveragePrice {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    //EXERCISE 4: Find the average price of "Sony DSC-S85 Digital Camera Cleaning Kit by

    val retail_data = sc.textFile("./data/retail_data")
    //Taking out only description and price fields
    val req_retail_data = retail_data.map(x => x.split("\\|"))
    val filter_retail_data = req_retail_data.filter(line => line.contains(
      "Sony DSC-S85 Digital Camera Cleaning Kit by General Brand"))
    val gen_split_retail_data = filter_retail_data.map(line => (line(3), line(2).toFloat))
    val sum_retail_data = gen_split_retail_data.mapValues(x => (x.toFloat, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val average = sum_retail_data.map { x =>
      val temp = x._2
      val total = temp._1
      val count = temp._2
      (x._1, total / count)
    }
    val averageCollection = average.collect()
    print(averageCollection(0))
  }
}