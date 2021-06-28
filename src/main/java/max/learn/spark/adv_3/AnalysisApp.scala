package max.learn.spark.adv_3

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * （接着上一个应用 LogEtlApp ，数据 ETL 存到 HDFS / HBase 之后，后续需要做统计分析）
  *
  * 使用Spark对HBase中的数据做统计分析操作
  *
  * 1） 统计每个国家每个省份的访问量
  * 2） 统计不同浏览器的访问量
  */
object AnalysisApp extends Logging {

  def main(args: Array[String]): Unit = {

    // 创建 Spark Session
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("AnalysisApp")
      .master("local[2]")
      .getOrCreate()

    // 获取要进行统计分析的日期
    val day = "20201010"

    // 连接 HBase
    val conf = new Configuration()
    conf.set("hbase.rootdir","hdfs://hadoop000:9000/hbase")
    conf.set("hbase.zookeeper.quorum","hadoop000:2181")

    // 查什么表
    val tableName = "access_" + day
    conf.set(TableInputFormat.INPUT_TABLE, tableName) // 要从哪个表里面去读取数据

    // 查什么列
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("data_cf"))
    scan.addColumn(Bytes.toBytes("data_cf"), Bytes.toBytes("country"))
    scan.addColumn(Bytes.toBytes("data_cf"), Bytes.toBytes("province"))
    scan.addColumn(Bytes.toBytes("data_cf"), Bytes.toBytes("browsername"))
    conf.set(TableInputFormat.SCAN, Base64.encodeBase64String(ProtobufUtil.toScan(scan).toByteArray))

    // 通过 Spark 的 newAPIHadoopRDD 读取数据
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    // 打印出来看一下
    hbaseRDD.take(5).foreach(x => {
      val rowKey = Bytes.toString(x._1.get())

      for(cell <- x._2.rawCells()) {
        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))

        println(s"$rowKey : $cf : $qualifier : $value")
      }
    })

    // Spark优化中最常用的一个点：Cache
    hbaseRDD.cache()

    // 需求1：统计每个国家每个省份的访问量  ==> TOP10

    logError("province check 1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    hbaseRDD.map(x => {
      val country = Bytes.toString(x._2.getValue("data_cf".getBytes, "country".getBytes))
      val province = Bytes.toString(x._2.getValue("data_cf".getBytes, "province".getBytes))
      ((country, province), 1)
    }).reduceByKey(_ + _)
      .map(x => (x._2, x._1)).sortByKey(false)
      .map(x => (x._2, x._1)).take(10).foreach(println)

    logError("province check 2~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    import spark.implicits._
    hbaseRDD.map(x => {
      val country = Bytes.toString(x._2.getValue("data_cf".getBytes, "country".getBytes))
      val province = Bytes.toString(x._2.getValue("data_cf".getBytes, "province".getBytes))
      CountryProvince(country, province)
    }).toDF.select("country","province")
      .groupBy("country","province").count().show(10,false)

    // 需求2：统计浏览器的访问量  ==> TOP10

    logError("browser check 1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    hbaseRDD.map(x => {
      val browsername = Bytes.toString(x._2.getValue("data_cf".getBytes, "browsername".getBytes))
      (browsername, 1)
    }).reduceByKey(_ + _)
      .map(x => (x._2, x._1)).sortByKey(false)
      .map(x => (x._2, x._1)).take(10).foreach(println)

    // spark sql
    logError("browser check 2~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    hbaseRDD.map(x => {
      val browsername = Bytes.toString(x._2.getValue("data_cf".getBytes, "browsername".getBytes))
      Browser(browsername)
    }).toDF().createOrReplaceTempView("tmp")

    spark.sql("select browsername,count(1) cnt from tmp group by browsername order by cnt desc limit 10").show(false)

    hbaseRDD.unpersist(true)

    spark.stop()
  }

  case class CountryProvince(country: String, province: String)

  case class Browser(browsername: String)

}
