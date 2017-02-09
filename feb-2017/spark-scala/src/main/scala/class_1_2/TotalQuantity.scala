import org.apache.spark.{SparkConf, SparkContext}

//EXERCISE 6: Count the total number of quantity for 1433144 product id.


object TotalQuantity {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "")
    val conf = new SparkConf().setAppName("Load from hdfs")

    val retail_data = sc.textFile("./data/retail_data")

    // filtering out records having "1433144" as product_id
    val  filter_retail_data = retail_data.filter(x => x.contains("1433144"))

    // Splitting a records and taking out only product id and quantity(int) columns
    val split_ret_data = filter_retail_data.map(x => x.split("\\|")).map( y => (y(0),y(1).toInt))

    // Grouping the above data by key i.e. product_id and adding up all the values corresponding to it
    val total_quantity = split_ret_data.groupByKey().mapValues(_.sum)

    // Displays the result
    println(total_quantity.collect()(0))

    sc.stop()
  }
}
