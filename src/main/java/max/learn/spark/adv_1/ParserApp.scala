package max.learn.spark.adv_1

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.SparkSession

/**
  * 使用 Spark 进行简单的数据清洗 （第三方类库 + 一个简单的手动实现）
  */
object ParserApp {

  def main(args: Array[String]): Unit = {

    // create spark
    val spark = SparkSession.builder().appName("TestApp").master("local[2]").getOrCreate()


    // print a dummy list
    println("1. a dummy list")
    val rdd = spark.sparkContext.parallelize(List(1,2,3,4))
    rdd.collect().foreach(println)

    val inputFile = "D:/codebase/hadoopez/data/test-access.log"

    // use a 3rd lib to format the file, find code here: https://coding.imooc.com/learn/list/357.html
    println("2. format file")
    System.setProperty("icode", "B42880DE94C66CC3")
    var logDF = spark.read
      .format("com.imooc.bigdata.spark.pk")
      .option("path",inputFile)
      .load()
    logDF.show(false)

    // use user defined function to format another column
    println("3. format time column")
    import org.apache.spark.sql.functions._
    def formatTime() = udf((time:String) =>{
      FastDateFormat.getInstance("yyyyMMddHHmm").format(
        new Date(FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
        .parse(time.substring(time.indexOf("[")+1, time.lastIndexOf("]"))).getTime
      ))
    })

    // 在已有的DF之上添加或者修改字段
    logDF = logDF.withColumn("formattime", formatTime()(logDF("time")))

    logDF.show(false)

    spark.stop()
  }

}
