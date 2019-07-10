package com.lumia.staypoint

import com.lumia.grid.Grid
import com.lumia.spark.SparkTool
import com.lumia.util.DateUtil
import org.apache.spark.sql.SaveMode

/**
  * @description 将融合表数据整合成停留表数据
  * @author lumia
  * @date 2019/7/10 15:06
  */
object StayPoint extends SparkTool {
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
    //分区参数
    val dayId = args(0)
    //融合表输入路径
    val mergeLocationInputPath = Constants.MERGELOCATION_INPUT_PATH + Constants.PARTITION_NAME + dayId

    Logger.info(s"融合表输入路径:$mergeLocationInputPath")

    val sqlContext = sparkSession.sqlContext

    //融合表dataFrame
    val mergeLocationDF = sqlContext.read.parquet(mergeLocationInputPath)

    /**
      * 融合表数据格式
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
      */
    //取出手机号、网格号、区县编码、停留时间
    val groupRDD = mergeLocationDF.select("mdn", "start_time", "county_id", "grid_id")
      .rdd.map(row => {
      val mdn = row.getAs("mdn").toString
      val start_time = row.getAs("start_time").toString
      val county_id = row.getAs("county_id").toString
      val grid_id = row.getAs("grid_id").toString
      val mdnAndGridAndCounty = mdn + "_" + grid_id + "_" + county_id
      (mdnAndGridAndCounty, start_time)
    }).groupByKey() //根据手机号与网格号进行分组

    val stayPointRDD = groupRDD.map(tuple => {
      val mdnAndGridAndCounty = tuple._1
      val times = tuple._2.toList
      val timeList = times.flatMap(_.split(",")).map(_.toLong).sorted
      val grid_first_time = timeList.head //在网格停留开始时间
      val grid_last_time = timeList.last //在网格停留结束时间
      //停留时长
      val duration = math.abs(DateUtil.betweenM(grid_last_time.toString, grid_first_time.toString))

      val split = mdnAndGridAndCounty.split("_")
      val mdn = split(0) //手机号
      val grid_id = split(1) //网格id
      val county_id = split(2) //区县编码

      val point = Grid.getCenter(grid_id.toLong)

      //网格中心点经度
      val longi = point.x
      //网格中心点纬度
      val lati = point.y

      /**
        * 停留表数据格式
        * mdn string comment '用户手机号码'
        * ,longi string comment '网格中心点经度'
        * ,lati string comment '网格中心点纬度'
        * ,grid_id string comment '停留点所在电信内部网格号'
        * ,county_id string comment '停留点区县'
        * ,duration string comment '机主在停留点停留的时间长度（分钟）,lTime-eTime'
        * ,grid_first_time string comment '网格第一个记录位置点时间（秒级）'
        * ,grid_last_time string comment '网格最后一个记录位置点时间（秒级）'
        */

      (mdn, longi, lati, grid_id, county_id, duration, grid_first_time, grid_last_time)
    })

    import sqlContext.implicits._

    //停留表输出路径
    val stayPointOutputPath = Constants.STAYPOINT_OUTPUT_PATH + Constants.PARTITION_NAME + dayId
    Logger.info(s"停留表输出路径:$stayPointOutputPath")

    //将停留表写入hdfs中
    stayPointRDD
      .toDF("mdn", "longi", "lati", "grid_id", "county_id", "duration", "grid_first_time", "grid_last_time")
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .parquet(stayPointOutputPath)

  }

  /**
    * 初始化spark的配置
    */
  override def init(): Unit = {
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
