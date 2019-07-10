/**
 * Copyright (C) China Telecom Corporation Limited, Cloud Computing Branch Corporation - All Rights Reserved
 * <p>
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * <p>
 * Proprietary and confidential
 * <p>
 * Contributors:
 * dingjingbo dingjb<dingjb@chinatelecom.cn>, 2015-12
 */
package com.lumia.util;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 加载省市县码表
 *
 * @author dingjingbo
 * @date 2015年8月17日
 */
public class SSXRelation {
    public static final Map<String, Set<String>> PROVINCE_CITIES = new HashMap<String, Set<String>>();
    public static final Map<String, Set<String>> CITY_COUNTIES = new HashMap<String, Set<String>>();
    public static final Map<String, String> CITY_PROVINCE = new HashMap<String, String>();
    public static final Map<String, String> CITY_LEVEL = new HashMap<String, String>();
    public static final Map<String, String> CITY_FEATURE1 = new HashMap<String, String>();
    public static final Map<String, String> CITY_ECONOMIC_BELT = new HashMap<String, String>();
    public static final Map<String, String> COUNTY_CITY = new HashMap<String, String>();
    public static final Map<String, String> COUNTY_TYPE = new HashMap<String, String>();
    public static final Map<String, String> CODE_NAME = new HashMap<String, String>();
    public static final Map<String, String> NAME_CODE = new HashMap<String, String>();
    public static final Map<String, String> COUNTY_PROVINCE = new HashMap<String, String>();

    static {
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            InputStream inputStream = SSXRelation.class.getClassLoader().getResourceAsStream("ssxdx.txt");
            reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            String line = "";
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(",");
                String provinceCode = split[0].trim().length() == 3 ? split[0].trim() : split[0].trim().substring(1);
                String provinceName = split[1];
                String cityCode = split[2].trim();
                String cityName = split[3].trim();
                String countyCode = split[4].trim();
                String countyName = split[5].trim();
                String city_level = split[6].trim();
                String economic_belt = split[7].trim();
                String city_feature1 = split[8].trim();
                String county_type = split[9].trim();
                Set<String> citycodes = PROVINCE_CITIES.get(provinceCode);
                if (citycodes == null) {
                    citycodes = new HashSet<String>();
                    PROVINCE_CITIES.put(provinceCode, citycodes);
                }
                citycodes.add(cityCode);
                Set<String> countycodes = CITY_COUNTIES.get(cityCode);
                if (countycodes == null) {
                    countycodes = new HashSet<String>();
                    CITY_COUNTIES.put(cityCode, countycodes);
                }
                countycodes.add(countyCode);
                CITY_PROVINCE.put(cityCode, provinceCode);
                COUNTY_CITY.put(countyCode, cityCode);
                CODE_NAME.put(provinceCode, provinceName);
                CODE_NAME.put(cityCode, cityName);
                CODE_NAME.put(countyCode, countyName);
                NAME_CODE.put(provinceName, provinceCode);
                NAME_CODE.put(cityName, cityCode);
                NAME_CODE.put(countyName, countyCode);
                CITY_LEVEL.put(cityCode, city_level);
                CITY_ECONOMIC_BELT.put(cityCode, economic_belt);
                CITY_FEATURE1.put(cityCode, city_feature1);
                COUNTY_TYPE.put(countyCode, county_type);
                COUNTY_PROVINCE.put(countyCode, provinceCode);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    public static void main(String[] args) {
        System.out.println(SSXRelation.CODE_NAME.get("81101"));
    }
}
