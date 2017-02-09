
import org.apache.spark.SparkContext

object AverageDividend {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "")
    val nyse_data = sc.textFile("./data/nyse")
    val split_nyse = nyse_data.map(line => line.split("\\t"))
    print(split_nyse.first())
    val gen_split_nyse = split_nyse.map(line => (line(1),line(3).toFloat))
    val sum_nyse_data = gen_split_nyse.mapValues(x => (x.toFloat,1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    print(sum_nyse_data.take(5))
    val average = sum_nyse_data.map{ x =>
      val temp = x._2
      val total = temp._1
      val count = temp._2
      (x._1,total/count)
    }
    average.collect()
  }
}