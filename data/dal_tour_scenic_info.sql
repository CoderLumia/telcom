CREATE EXTERNAL TABLE IF NOT EXISTS dal_tour.dal_tour_scenic_info (
    scenic_Id string comment '景区ID'  
    ,scenic_name string comment '景区名称'  
    ,level string comment '景区级别，4A，5A...'  
    ,types string comment '景区类型'  
    ,loc_type string comment '景区位置类型'  
    ,boundary string comment '边界坐标'  
) 
comment  '景区配置表'
ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/subtl/dal/tour/dal_tour_scenic_info'; 



