package com.lumia.tour;

import com.lumia.Config;

/**
 * @author lumia
 * @description 常量类
 * @date 2019/7/10 17:16
 */
public final class Constants {
    private Constants(){}

    //停留表输入路径
    public final static String STAYPOINT_INPUT_PATH = Config.getString("staypoint.input.path");
    //用户画像表输入路径
    public final static String USERTAG_INPUT_PATH = Config.getString("usertag.input.path");
    //省游客表输出路径
    public final static String PROVINCE_OUTPUT_PATH = Config.getString("province.output.path");
    //省游客输入路径
    public final static String PROVINCE_INPUT_PATH = Config.getString("province.input.path");
    //市游客输出路径
    public final static String CITY_INPUT_PATH = Config.getString("city.output.path");
    //市游客输入路径
    public final static String CITY_OUTPUT_PATH = Config.getString("city.input.path");
    //日分区名
    public final static String PARTITION_NAME_DAY = "/day_id=";
    //月分区名
    public final static String PARTITION_NAME_MONTH = "/month_id=";
    //redis的host
    public final static String REDIS_HOST= Config.getString("redis.host");
    //redis端口
    public final static Integer REDIS_PORT = Config.getInt("redis.port");


}
