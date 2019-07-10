package com.lumia.tour

import com.lumia.spark.SparkTool
import com.lumia.util.{Geography, SSXRelation}
import org.apache.spark.sql.SaveMode

/**
  * @description 省游客表数据生成
  * @author lumia
  * @date 2019/7/10 17:21
  */
object DalTourProvince extends SparkTool {
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
    //天分区
    val dayId = args(0)
    //停留表输入路径
    val staypointInputPath = Constants.STAYPOINT_INPUT_PATH + Constants.PARTITION_NAME_DAY + dayId
    Logger.info(s"停留表输入路径:$staypointInputPath")
    if (args(1) == null || args(1).trim.isEmpty) {
      Logger.error("请传入用户画像表月分区")
      throw new IllegalArgumentException("请传入用户画像表月分区")
    }
    //月分区
    val monthId = args(1)
    //用户画像表输入路径
    val userTagInputPath = Constants.USERTAG_INPUT_PATH + Constants.PARTITION_NAME_MONTH + monthId
    Logger.info(s"用户画像表的输入路径:$userTagInputPath")

    val sqlContext = sparkSession.sqlContext

    val staypointDF = sqlContext.read.parquet(staypointInputPath)

    /**
      * 停留表数据格式
      * +--------------------+------------------+------------------+---------------+---------+--------+---------------+--------------+
      * |                 mdn|             longi|              lati|        grid_id|county_id|duration|grid_first_time|grid_last_time|
      * +--------------------+------------------+------------------+---------------+---------+--------+---------------+--------------+
      * |48E9E1D650EA71977...|          117.2425|           31.9775|117240031975040|  8340121|      320| 20180503150218|20180503145418|
      *
      *
      */
    val userTagDF = sqlContext.read.parquet(userTagInputPath)
    /**
      *
      * 用户画像表数据格式
      * +--------------------+--------------------+------+---+--------------------+-----------+-----------+-----------+-----+------+---------------+--------------+
      * |                 mdn|                name|gender|age|           id_number|number_attr|trmnl_brand|trmnl_price|packg|conpot|   resi_grid_id|resi_county_id|
      * +--------------------+--------------------+------+---+--------------------+-----------+-----------+-----------+-----+------+---------------+--------------+
      * |1D2916F9ACFBFA279...|AA3DB6AB27731E170...|     2| 20|AA3DB6AB27731E170...|      86322|         魅族|       4300|   29|     9|117255031870040|       8340104|
      *
      */
    //以手机号进行连接
    val joinDF = staypointDF.join(userTagDF, "mdn")

    val kvRDD = joinDF.rdd.map(row => {
      //手机号
      val mdn = row.getAs[String]("mdn")
      //停留点区县
      val county_id = row.getAs[String]("county_id")
      //停留点网格
      val grid_id = row.getAs[String]("grid_id")
      //停留时间
      val duration = row.getAs[Integer]("duration")
      //常住地网格
      val resi_grid_id = row.getAs[String]("resi_grid_id")
      //常住地区县
      val resi_county_id = row.getAs[String]("resi_county_id")

      //停留点省id
      val provinceId = SSXRelation.COUNTY_PROVINCE.get(county_id)

      val key = mdn + "\t" + provinceId + "\t" + resi_county_id
      (key, s"$grid_id\t$duration\t$resi_grid_id")
    })

    /**
      * 出行距离大于10km
      * 在省内停留时间大于3个小时
      */
    val filterRDD = kvRDD
      .groupByKey()
      .map(tuple => {
        val mdnAndProvinceAndCounty = tuple._1.split("\t")
        val mdn = mdnAndProvinceAndCounty(0)
        val provinceId = mdnAndProvinceAndCounty(1)
        val resiCountyId = mdnAndProvinceAndCounty(2)

        //计算出行最远的点
        val points = tuple._2.toList

        val distances = points.map(line => {
          val split = line.split("\t")
          //目的地网格
          val grid_id = split(0).toLong
          //常住点网格
          val resi_grid_id = split(2).toLong
          //计算距离
          val distance = Geography.calculateLength(grid_id, resi_grid_id)
          distance
        })

        //最远距离
        val maxDistance = distances.max

        val sumDuration = points.map(line => {
          line.split("\t")(1).toInt
        }).sum
        //返回值
        (mdn, resiCountyId, provinceId, sumDuration.toDouble, maxDistance)
      }).filter(tuple => {
      val sumDuration = tuple._4
      val maxDistance = tuple._5
      sumDuration > 180 && maxDistance > 10000
    })

    import sqlContext.implicits._

    /**
      * mdn string comment '手机号大写MD5加密'
      * ,source_county_id string comment '游客来源区县'
      * ,d_province_id string comment '旅游目的地省代码'
      * ,d_stay_time double comment '游客在该省停留的时间长度（小时）'
      * ,d_max_distance double comment '游客本次出游距离'
      */

    val provinceOutputPath = Constants.PROVINCE_OUTPUT_PATH + Constants.PARTITION_NAME_DAY + dayId
    Logger.info(s"省游客表输出路径:$provinceOutputPath")
    filterRDD
      .toDF("mdn", "source_county_id", "d_province_id", "d_stay_time", "d_max_distance")
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .parquet(provinceOutputPath)
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
