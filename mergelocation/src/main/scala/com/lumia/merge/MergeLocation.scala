package com.lumia.merge

import com.lumia.spark.SparkTool
import org.apache.spark.sql.SaveMode

/**
  * @description 将flume采集的四类数据进行合并，并写入到merge表中
  * @author lumia
  * @date 2019/7/10 11:30
  */
object MergeLocation extends SparkTool {
  /**
    * 在run方法中编写spark的业务逻辑
    *
    * @param args 运行参数
    */
  override def run(args: Array[String]): Unit = {
    if (args.length == 0) {
      Logger.error("请指定分区参数")
      throw new IllegalArgumentException("请指定分区参数")
    }

    val dayId: String = args(0)

    /**
      * 构建数据的输入路径
      */
    val ddrInputPath = Constants.DDR_INPUT_PATH + Constants.PARTITION_NAME + dayId
    val dpiInputPath = Constants.DPI_INPUT_PATH + Constants.PARTITION_NAME + dayId
    val wcdrInputPath = Constants.WCDR_INPUT_PATH + Constants.PARTITION_NAME + dayId
    val oiddInputPath = Constants.OIDD_INPUT_PATH + Constants.PARTITION_NAME + dayId

    Logger.info(s"DDR输入路径:$ddrInputPath")
    Logger.info(s"DPI输入路径:$dpiInputPath")
    Logger.info(s"WCDR输入路径:$wcdrInputPath")
    Logger.info(s"OIDD输入路径:$oiddInputPath")
    //获取sparkContext
    val sparkContext = sparkSession.sparkContext
    //获取数据RDD
    val ddrRDD = sparkContext.textFile(ddrInputPath)
    val dpiRDD = sparkContext.textFile(dpiInputPath)
    val wcdrRDD = sparkContext.textFile(wcdrInputPath)
    val oiddRDD = sparkContext.textFile(oiddInputPath)

    //将四类数据合并成融合表
    val mergeRDD = ddrRDD.union(dpiRDD).union(wcdrRDD).union(oiddRDD)

    val mergeLocationOutputPath = Constants.MERGELOCATION_OUTPUT_PATH + Constants.PARTITION_NAME + dayId

    Logger.info(s"融合表的输入路径:$mergeLocationOutputPath")

    val sqlContext = sparkSession.sqlContext
    //导入隐式转换
    import sqlContext.implicits._

    /**
      * 数据字段
      * mdn string comment '手机号码'
      * ,start_time string comment '业务开始时间'
      * ,county_id string comment '区县编码'
      * ,longi string comment '经度'
      * ,lati string comment '纬度'
      * ,bsid string comment '基站标识'
      * ,grid_id string comment '网格号'
      * ,biz_type string comment '业务类型'
      * ,event_type string comment '事件类型'
      * ,data_source string comment '数据源'
      *
      */
    val mergeDF = mergeRDD.map(_.split(Constants.DATA_SPLIT)).map(dataArr => {
      val (mdn, start_time, county_id, longi, lati, bsid, grid_id, biz_type, event_type, data_source) =
        (dataArr(0), dataArr(1), dataArr(2), dataArr(3), dataArr(4), dataArr(5), dataArr(6), dataArr(7), dataArr(8), dataArr(9))
      (mdn, start_time, county_id, longi, lati, bsid, grid_id, biz_type, event_type, data_source)
    }).toDF("mdn", "start_time", "county_id", "longi", "lati", "bsid", "grid_id", "biz_type", "event_type", "data_source")

    /**
      * 将融合表写入到hdfs中
      */
    mergeDF
      .write
      //覆盖
      .mode(saveMode = SaveMode.Overwrite)
      //指定文件的为parquet格式，会对数据进行压缩
      .parquet(mergeLocationOutputPath)

  }

  /**
    * 初始化spark的配置
    */
  override def init(): Unit = {
    builder.master("local[4]")
    //spark  shuffle  过程数据落地缓存内存大小
    builder.config("spark.shuffle.file.buffer", "64k")
    //reduce去map中一次最多拉去多少数据
    builder.config("spark.reducer.maxSizeInFlight", "96m")
    //shuffle read task从shuffle write task所在节点拉取属于自己的数据时  重试次数
    builder.config("spark.shuffle.io.maxRetries", "10")
    //shuffle read task从shuffle write task所在节点拉取属于自己的数据时  等待时间
    builder.config("spark.shuffle.io.retryWait", "60s")

    //两个一起调节，剩下0.2是task运行时可以使用的内存
    //启用内存管理模式，使下面的内存分配生效
    builder.config("spark.memory.useLegacyMode", "true")
    // shuffle  内存占比
    builder.config("spark.shuffle.memoryFraction", "0.4")
    //  RDD持久化可以使用的内存
    builder.config("spark.storage.memoryFraction", "0.4")

  }
}
