package max.learn.spark.basic

import org.apache.spark.sql.SparkSession

// read one file from local system and count a,b occurrences using Spark
// 可以本地运行
object HelloWorld {
  def main(args: Array[String]) {

    // input file
    val logFile = "D:/codebase/bigdataez/data/spark/sample_file.txt"

    // create spark
    val spark = SparkSession.builder
        .appName("Simple Char Count Application")
        .config("spark.master", "local")
        .getOrCreate()

    // read data
    val logData = spark.read.textFile(logFile).cache()

    // count
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    // output
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }
}