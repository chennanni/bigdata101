package max.learn.spark.adv_2

import java.util.zip.CRC32
import java.util.{Date, Locale}

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  *
  * 对日志进行 ETL 操作：
  * 1. 把数据从文件系统(本地、HDFS)读取出来
  * 2. 进行清洗(ip/ua/time) - 类似 ParserApp
  * 3. 最终存储到HBase中
  *
  * 批处理：一天处理一次，今天凌晨来处理昨天的数据
  * 需要传给我们的 ImoocLogApp 一个处理时间：yyyyMMdd
  * HBase表：一天一个，logs_yyyyMMdd
  *p
  * 改进点2：使用 Spark submit 在服务器上提交作业，禁用 WAL 加速数据处理；参考 coding357/ImoocLogV2App
  *   put.setDurability(Durability.SKIP_WAL)
  *   flushTable(tableName, conf)
  * 改进点3：使用 Spark 直接生成 Hfile，最后再 load 进 Hbase；参考 coding357/ImoocLogV3App
  *
  */
object LogEtlApp extends Logging {

  // need to start HBase $ Zookeeper in Server
  val hbase_path = "hdfs://hadoop000:9000/hbase"
  val hbase_file = "hdfs://hadoop000:9000/etl/access/hbase"
  val zk_path = "hadoop000:2181"

  // 课程 lib 代码，在这里 https://coding.imooc.com/learn/list/357.html
  val code = "7C151EF3877A82AF"

  // 获取目标文件
  val input = "D:/codebase/bigdataez/data/spark/test-access.log"
  //val input = s"hdfs://hadoop000:8020/access/$day/*"

  def main(args: Array[String]): Unit = {

    // 获取处理日期
//    if(args.length != 1) {
//      println("Usage: ImoocLogApp <time>")
//      System.exit(1)
//    }
    //val day = args(0)
    val day = "20190130"

    // 创建 Spark Session
    val spark = SparkSession.builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("ImoocLogApp").master("local[2]")
      .getOrCreate()
    //val spark = SparkSession.builder().getOrCreate()

    // 调用现有 lib 预处理文件
    System.setProperty("icode", code)
    var logDF = spark.read.format("com.imooc.bigdata.spark.pk").option("path",input).load()

    // 额外处理时间 field
    import org.apache.spark.sql.functions._
    def formatTime() = udf((time:String) =>{
      FastDateFormat.getInstance("yyyyMMddHHmm").format(
        new Date(FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
          .parse(time.substring(time.indexOf("[")+1, time.lastIndexOf("]"))).getTime
        ))
    })
    logDF = logDF.withColumn("formattime", formatTime()(logDF("time")))

    // 打印预处理的数据
    logDF.show(true)

    // ------以上部分已经将我们所需要处理的日志信息进行了处理(ip/ua/time)--------

    // 数据清洗完了，下一步应该是将数据落地到HBase中（哪些字段属于哪个cf、表名、rowkey）

    val hbaseInfoRDD = logDF.rdd.map(x => {

      // 获取 field
      val ip = x.getAs[String]("ip")
      val country = x.getAs[String]("country")
      val province = x.getAs[String]("province")
      val city = x.getAs[String]("city")
      val formattime = x.getAs[String]("formattime")
      val method = x.getAs[String]("method")
      val url = x.getAs[String]("url")
      val protocal = x.getAs[String]("protocal")
      val status = x.getAs[String]("status")
      val bytessent = x.getAs[String]("bytessent")
      val referer = x.getAs[String]("referer")
      val browsername = x.getAs[String]("browsername")
      val browserversion = x.getAs[String]("browserversion")
      val osname = x.getAs[String]("osname")
      val osversion = x.getAs[String]("osversion")
      val ua = x.getAs[String]("ua")

      // 放到一个 HashMap 中
      val columns = scala.collection.mutable.HashMap[String,String]()
      columns.put("ip",ip)
      columns.put("country",country)
      columns.put("province",province)
      columns.put("city",city)
      columns.put("formattime",formattime)
      columns.put("method",method)
      columns.put("url",url)
      columns.put("protocal",protocal)
      columns.put("status",status)
      columns.put("bytessent",bytessent)
      columns.put("referer",referer)
      columns.put("browsername",browsername)
      columns.put("browserversion",browserversion)
      columns.put("osname",osname)
      columns.put("osversion",osversion)

      // 准备 HBase row key
      val rowkey = getRowKey(day, referer+url+ip+ua)
      // 准备 要保存到 HBase 的 Put 对象
      val put = new Put(Bytes.toBytes(rowkey))
      for((k,v) <- columns) {
        put.addColumn(Bytes.toBytes("data_cf"), Bytes.toBytes(k.toString), Bytes.toBytes(v.toString));
      }
      // 准备写入对象
      (new ImmutableBytesWritable(rowkey.getBytes), put)
    })
    //.collect().foreach(println)

    // 配置 Hbase, ZK
    val conf = new Configuration()
    conf.set("hbase.rootdir",hbase_path)
    conf.set("hbase.zookeeper.quorum",zk_path)

    // 设置写数据到哪个表中
    val tableName = createTable(day, conf)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // 保存数据
    hbaseInfoRDD.saveAsNewAPIHadoopFile(
      hbase_file,
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )

    logInfo(s"作业执行成功... $day")

    spark.stop()
  }

  // rowKey = time_checksum(info)
  def getRowKey(time:String, info:String) = {
    // part 1: time
    val builder = new StringBuilder(time)
    builder.append("_")

    // part 2: info的checksum值
    val crc32 = new CRC32()
    crc32.reset()
    if(StringUtils.isNotEmpty(info)){
      crc32.update(Bytes.toBytes(info))
    }
    builder.append(crc32.getValue)

    builder.toString()
  }

  def createTable(day:String, conf:Configuration) ={
    val table = "access_" + day

    var connection:Connection = null
    var admin:Admin = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin

      /**
        * 这个Spark作业是离线的，然后一天运行一次，如果中间处理过程中有问题
        * 下次重跑的时候，是不是应该先把表数据清空，然后重新写入
        */
      val tableName = TableName.valueOf(table)
      if(admin.tableExists(tableName)) {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }

      val tableDesc = new HTableDescriptor(TableName.valueOf(table))
      val columnDesc = new HColumnDescriptor("data_cf")
      tableDesc.addFamily(columnDesc)
      admin.createTable(tableDesc)
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if(null != admin) {
        admin.close()
      }

      if(null != connection) {
        connection.close()
      }
    }

    table
  }
}
