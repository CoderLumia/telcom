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

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 时间工具类
 *
 * @author qinxiao
 */
public class DateUtil {
    private DateUtil() {

    }

    /**
     * 在一个日期上加上或件去一个天数，得到新的日期
     *
     * @param date
     * @param day
     * @return
     */
    public static Date add(Date date, int day) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, day);
        return calendar.getTime();
    }

    /**
     * 计算两个日期中间的日期
     *
     * @param startDate
     * @param endDate
     * @return
     */
    public static List<Date> calculateDays(Date startDate, Date endDate) {
        List<Date> dates = new ArrayList<Date>();
        if (endDate.getTime() < startDate.getTime()) {
            return dates;
        }
        dates.add(startDate);
        for (int i = 1; i < Integer.MAX_VALUE; i++) {
            Date add = add(startDate, i);
            if (add.getTime() >= endDate.getTime()) {
                break;
            } else {
                dates.add(add);
            }
        }
        dates.add(endDate);
        return dates;

    }

    /**
     * 计算两个日期中间的日期
     *
     * @param startDate
     * @param endDate
     * @return
     */
    public static Set<String> calculateDays(String startDate, String endDate, String pattern) {
        Set<String> dates = new TreeSet<String>();
        Date convertStartDate = convertString2Date(startDate, pattern);
        Date convertEndDate = convertString2Date(endDate, pattern);
        if (convertEndDate.getTime() < convertStartDate.getTime()) {
            return dates;
        }
        dates.add(convertDate2String(convertStartDate, "yyyyMMdd"));
        for (int i = 1; i < Integer.MAX_VALUE; i++) {
            Date add = add(convertStartDate, i);
            if (add.getTime() >= convertEndDate.getTime()) {
                break;
            } else {
                dates.add(convertDate2String(add, "yyyyMMdd"));
            }
        }
        dates.add(convertDate2String(convertEndDate, "yyyyMMdd"));
        return dates;

    }

    /**
     * 日期格式转换
     *
     * @param date
     * @param pattern
     * @return
     */
    public static String convertDate2String(Date date, String pattern) {
        if (date == null) {
            return "";
        }
        SimpleDateFormat sf = new SimpleDateFormat(pattern);
        return sf.format(date);
    }

    /**
     * 日期格式转换
     *
     * @param date
     * @return
     */
    public static String convertDate2String(Date date) {
        return convertDate2String(date, "yyyy-MM-dd");
    }

    /**
     * 字符串转化成日期
     *
     * @param date
     * @return
     */
    public static Date convertString2Date(String date) {
        return convertString2Date(date, "yyyy-MM-dd");
    }

    /**
     * 字符串转化成日期
     *
     * @param date
     * @return
     */
    public static Date convertString2Date(String date, String pattern) {
        try {
            SimpleDateFormat sf = new SimpleDateFormat(pattern);
            return sf.parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 比较两个日期之间的大小
     *
     * @param d1
     * @param d2
     * @return 前者大于或等于后者返回true 反之false
     */
    public static boolean compareDate(Date d1, Date d2) {
        return d1.compareTo(d2) >= 0;
    }

    /**
     * 比较当前时间与指定时间的大小
     *
     * @param date 指定时间
     * @return 指定时间大于或等于当前时间返回true 反之false
     */
    public static boolean compareDate(Date date) {
        return compareDate(date, new Date());
    }

    /**
     * 计算两个日期之间相隔的天数
     *
     * @param startDate
     * @param endDate
     * @return
     */
    public static int betweenDay(Date startDate, Date endDate) {
        try {
            long startTime = new SimpleDateFormat().parse(convertDate2String(startDate)).getTime();
            long endTime = new SimpleDateFormat().parse(convertDate2String(endDate)).getTime();
            return (int) ((endTime - startTime) / 1000 / 60 / 60 / 24);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 计算两个时间之间差的分钟数
     *
     * @param startDate
     * @param endDate
     * @return
     */
    public static int betweenM(String startDate, String endDate) {
        try {

            SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmmss");
            long startTime = sf.parse(startDate).getTime();
            long endTime = sf.parse(endDate).getTime();
            return (int) ((endTime - startTime) / 1000 / 60);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 说明：把Date对象转为XMLGregorianCalendar
     *
     * @return XMLGregorianCalendar
     * @author 创建时间：
     */
    public static XMLGregorianCalendar convertToXMLGregorianCalendar(Date date) {

        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        XMLGregorianCalendar gc = null;
        try {
            gc = DatatypeFactory.newInstance().newXMLGregorianCalendar(cal);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return gc;
    }

    /**
     * 如果该天为月末返回月份,其他任何情况返回null
     *
     * @param time yyyyMMdd
     * @return
     */
    public static String getMonthByEndDay(String time) {
        try {
            Calendar instance = Calendar.getInstance();
            instance.setTime(DateUtil.convertString2Date(time, "yyyyMMdd"));
            instance.add(Calendar.DAY_OF_MONTH, 1);
            if (instance.get(Calendar.DAY_OF_MONTH) == 1) {//每个月的最后一天统计当月数据
                instance.add(Calendar.MONTH, -1);
                String month = DateUtil.convertDate2String(instance.getTime(), "yyyyMM");
                return month;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}
