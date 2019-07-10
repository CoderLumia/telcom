package com.lumia.staypoint;

import com.lumia.Config;

/**
 * @author lumia
 * @description 常量类
 * @date 2019/7/10 15:01
 */
public final class Constants {
    private Constants() {}

    //融合表输入路径
    protected static final String MERGELOCATION_INPUT_PATH = Config.getString("mergelocation.input.path");

    //停留表输出路径
    protected static final String STAYPOINT_OUTPUT_PATH = Config.getString("staypoint.output.path");

    //分区名
    protected static final String PARTITION_NAME = "/day_id=";



}
