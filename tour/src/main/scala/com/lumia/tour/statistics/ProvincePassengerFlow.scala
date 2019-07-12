package com.lumia.tour.statistics

import com.lumia.spark.SparkTool
import com.lumia.tour.Constants
import com.lumia.tour.util.IndexToRedis

/**
  * @description 省客流量统计
  * @author lumia
  * @date 2019/7/10 19:20
  */
object ProvincePassengerFlow extends SparkTool {
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
    val provinceTourInputPath = Constants.PROVINCE_INPUT_PATH + Constants.PARTITION_NAME_DAY + dayId
    Logger.info(s"省游客表输入路径:$provinceTourInputPath")

    if (args(1) == null || args(1).trim.isEmpty) {
      Logger.error("请指定用户画像表月分区")
      throw new IllegalArgumentException("请指定用户画像表月分区")
    }
    //用户画像表月分区
    val monthId = args(1)

    val userTagInputPath = Constants.USERTAG_INPUT_PATH + Constants.PARTITION_NAME_MONTH + monthId

    Logger.info(s"用户画像表的输入路径:$userTagInputPath")

    val sqlContext = sparkSession.sqlContext

    /**
      * 省游客表数据
      * mdn string comment '手机号大写MD5加密'
      * ,source_county_id string comment '游客来源区县'
      * ,d_province_id string comment '旅游目的地省代码'
      * ,d_stay_time double comment '游客在该省停留的时间长度（小时）'
      * ,d_max_distance double comment '游客本次出游距离'
      */
    val provinceDF = sqlContext.read.parquet(provinceTourInputPath)


    /**
      * mdn string comment '手机号大写MD5加密'
      * ,name string comment '姓名'
      * ,gender string comment '性别，1男2女'
      * ,age string comment '年龄'
      * ,id_number string comment '证件号码'
      * ,number_attr string comment '号码归属地'
      * ,trmnl_brand string comment '终端品牌'
      * ,trmnl_price string comment '终端价格'
      * ,packg string comment '套餐'
      * ,conpot string comment '消费潜力'
      * ,resi_grid_id string comment '常住地网格'
      * ,resi_county_id string comment '常住地区县'
      */
    val userTagDF = sqlContext.read.parquet(userTagInputPath)

    /**
      * 省游客表与用户画像表关联
      */
    val joinDF = provinceDF.join(userTagDF, "mdn")

    /**
      * 将join后的进行缓存
      */
    joinDF.cache()


    def saveDataByColumn(column: String) = {
      if (column != null) {
        IndexToRedis.saveDataToRedis(joinDF, dayId, "d_province_id", column)
      } else {
        IndexToRedis.saveDataToRedis(joinDF, dayId, "d_province_id")
      }
    }

    /**
      * 客流量按天 [省id,客流量]
      */
    saveDataByColumn(null)


    /**
      * 性别按天 [省id,性别,客流量]
      */
    saveDataByColumn("gender")

    /**
      * 年龄按天 [省id,年龄,客流量]
      */
    saveDataByColumn("age")

    /**
      * 常住地按天 [省id,常住地市,客流量]
      */
    saveDataByColumn("resi_county_id")

    /**
      * 归属地按天 [省id,归属地市,客流量]
      */
    saveDataByColumn("source_county_id")

    /**
      * 终端型号按天 [省id,终端型号,客流量]
      */
    saveDataByColumn("trmnl_brand")

    /**
      * 消费等级按天 [省id,消费等级,客流量]
      */
    saveDataByColumn("conpot")

    /**
      * 停留时长按天 [省id,停留时长,客流量]
      */
    saveDataByColumn("d_stay_time")
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
  // sql的shuffle分区数量
  builder.config("spark.sql.shuffle.partitions", "10")
}
}
