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
    protected final static String STAYPOINT_INPUT_PATH = Config.getString("staypoint.input.path");
    //用户画像表输入路径
    protected final static String USERTAG_INPUT_PATH = Config.getString("usertag.input.path");
    //省游客表输出路径
    protected final static String PROVINCE_OUTPUT_PATH = Config.getString("province.output.path");
    //日分区名
    protected final static String PARTITION_NAME_DAY = "/day_id=";
    //月分区名
    protected final static String PARTITION_NAME_MONTH = "/month_id=";



}
