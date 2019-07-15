CREATE EXTERNAL TABLE IF NOT EXISTS dal_tour.dal_tour_scenic (
    scenic_name string comment '景区名称'  
    ,grid_id string comment '景区网格'  
) 
comment  '景区配置表'
ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dal_tour/dal_tour/dal_tour_scenic'; 



