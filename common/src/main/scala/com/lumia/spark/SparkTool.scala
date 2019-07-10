package com.lumia.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * @description
  * @author lumia
  * @date 2019/7/10 10:27
  */
abstract class SparkTool {
  var sparkSession:SparkSession = _
  var builder:SparkSession.Builder = _
  var Logger:Logger = _

  def main(args: Array[String]): Unit = {
    //创建日志对象
    Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

    builder = SparkSession.builder()
    //设置app的名称为当前类名
    builder.appName(this.getClass.getSimpleName)

    init()
    //创建SparkSession对象
    sparkSession = builder.getOrCreate();

    Logger.info("==============开始执行spark程序=============")

    //调用子类的run方法执行spark业务逻辑
    run(args)

    Logger.info("==============spark程序执行完成=============")
  }

  /**
    * 在run方法中编写spark的业务逻辑
    * @param args
    */
  def run(args: Array[String])

  /**
    * 初始化spark的配置
    */
  def init()
}
