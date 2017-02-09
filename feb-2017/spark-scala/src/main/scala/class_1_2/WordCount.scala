import org.apache.spark.{SparkConf, SparkContext}



object WordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "")
    val conf = new SparkConf().setAppName("")

    val rdd = sc.textFile("./data/war1.txt")
    val counts = rdd.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.collect()
    println(counts)

    sc.stop()
  }
}
