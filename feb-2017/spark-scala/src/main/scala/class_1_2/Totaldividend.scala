import org.apache.spark.{SparkConf, SparkContext}

// EXERCISE 3: Find Total Dividend declared by all companies


object Totaldividend {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val conf = new SparkConf().setAppName("Load from hdfs")
    val nyse_data = sc.textFile("./data/nyse")

    val split_nyse = nyse_data.map(line => line.split("\\t"))
    val gen_split_nyse = split_nyse.map(line => (line(1),line(3).toFloat))
    val total_dividends = gen_split_nyse.groupByKey().mapValues(_.sum)
    val itr = total_dividends.collect().iterator
    while (itr.hasNext) {
      println(itr.next().toString())
    }
    sc.stop()
  }
}
