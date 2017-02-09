import org.apache.spark.{SparkConf, SparkContext}
//EXERCISE 4: Find the top 5 companies by the total dividend declared.
object Top5Companies {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "")
    val nyse_data = sc.textFile("./data/nyse")
    val split_nyse = nyse_data.map(line => line.split("\\t"))
    val gen_split_nyse = split_nyse.map(line => (line(1),line(3).toFloat))
    val total_dividends = gen_split_nyse.groupByKey().mapValues(_.sum)
    val top_total_divi = total_dividends.sortBy(_._2,ascending=false)
    top_total_divi.take(5).foreach(println)
    sc.stop()
  }
}
