package com.lumia.merge;

import com.lumia.Config;

/**
 * @description 常量类
 * @author lumia
 * @date 2019/7/10 11:15
 */
public final class Constants {

    private Constants(){}

    /**
     * ddr dpi wcdr oidd 数据输入路径
     */
    protected static final String DDR_INPUT_PATH = Config.getString("ddr.input.path");
    protected static final String DPI_INPUT_PATH = Config.getString("dpi.input.path");
    protected static final String OIDD_INPUT_PATH = Config.getString("oidd.input.path");
    protected static final String WCDR_INPUT_PATH = Config.getString("wcdr.input.path");


    /**
     * 融合表路径
     */
    protected static final String MERGELOCATION_OUTPUT_PATH = Config.getString("mergelocation.output.path");

    /**
     * 分区名
     */
    protected static final String PARTITION_NAME = "/day_id=";

    /**
     * 数据分隔符
     */
    protected static final String DATA_SPLIT = Config.getString("data.split");

}
