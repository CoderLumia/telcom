package com.lumia.tour.util

import com.lumia.tour.Constants
import org.apache.spark.sql.DataFrame
import redis.clients.jedis.Jedis

/**
  * @description 将数据写入redis的工具类
  * @author lumia
  * @date 2019/7/11 09:47
  */
object IndexToRedis {

  /**
    * 将指标数据写入到redis中
    *
    * @param dataFrame  游客表与用户画像表join
    * @param dayId      天分区
    * @param itemId     分类id
    * @param columns    条件指标
    */
  def saveDataToRedis(dataFrame: DataFrame, dayId: String, itemId: String, columns: String*): Unit = {
    var column: String = null
    if (columns.nonEmpty) {
      column = columns.toList.head
    }
    var groupDF: DataFrame = null
    if (column != null) {
      groupDF = dataFrame.groupBy(itemId, column).count()
    } else {
      groupDF = dataFrame.groupBy(itemId).count()
    }
    groupDF.rdd.map(row => {
      val pid = row.getAs[String](itemId)
      var cName: String = null
      if (column != null) {
        cName = row.getAs[String](column)
      }
      val flow = row.getAs[Long]("count")
      if (cName != null) {
        (pid, cName + ":" + flow)
      } else {
        (pid, flow)
      }
    })
      //将分区的数据进行聚合
      .reduceByKey(_ + "|" + _)
      .foreachPartition(iter => {
        val jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT)
        //选择redis数据库索引
        jedis.select(1)
        iter.foreach(tuple => {
          val key = tuple._1 + dayId
          if (column != null) {
            jedis.hset(key, column + "_flow", tuple._2.toString)
          } else {
            jedis.hset(key, "flow", tuple._2.toString)
          }
        })
      })
  }

}
