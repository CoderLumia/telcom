/*********************************************************************
 *
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ______________________________________________________________
 *
 *  [2015] - [2020] China Telecom Corporation Limited, 
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of China Telecom Corporation and its suppliers,
 * if any. The intellectual and technical concepts contained 
 * herein are proprietary to China Telecom Corporation and its 
 * suppliers and may be covered by China and Foreign Patents,
 * patents in process, and are protected by trade secret  or 
 * copyright law. Dissemination of this information or 
 * reproduction of this material is strictly forbidden unless prior 
 * written permission is obtained from China Telecom Corporation.
 **********************************************************************/
package com.lumia.util;




import com.lumia.grid.Grid;

import java.awt.geom.Point2D;

/**
 * 经纬度坐标算法工具类
 * Copyright (C) China Telecom Corporation Limited, Cloud Computing Branch Corporation - All Rights Reserved
 * <p>
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * <p>
 * Proprietary and confidential
 * <p>
 * Contributors:
 * 83644, dingjb@chinatelecom.cn, 2015
 */
public class Geography {
    /**
     * 私有构造方法
     */
    private Geography() {

    }

    /**
     * 地球半径
     */
    private static final double EARTH_RADIUS = 6378137;


    /**
     * 计算两个经纬度之间的距离
     *
     * @param p1 第一个网格点
     * @param p2 第二个网格点
     * @return 返回两个点的距离
     */
    public static double calculateLength(Long p1, Long p2) {
        Point2D.Double point1 = Grid.getCenter(p1);
        Point2D.Double point2 = Grid.getCenter(p2);
        return calculateLength(point1.x, point1.y, point2.x, point2.y);
    }

    /**
     * 计算两个经纬度之间的距离
     *
     * @param longi1 经度1
     * @param lati1  纬度1
     * @param longi2 经度2
     * @param lati2  纬度2
     * @return 距离
     */
    public static double calculateLength(double longi1, double lati1, double longi2, double lati2) {
        double er = EARTH_RADIUS; // 地球半径
        double lat21 = lati1 * Math.PI / 180.0;
        double lat22 = lati2 * Math.PI / 180.0;
        double a = lat21 - lat22;
        double b = (longi1 - longi2) * Math.PI / 180.0;
        double sa2 = Math.sin(a / 2.0);
        double sb2 = Math.sin(b / 2.0);
        double d = 2 * er * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(lat21) * Math.cos(lat22) * sb2 * sb2));
        return Math.abs(d);
    }

}
