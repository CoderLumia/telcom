CREATE EXTERNAL TABLE IF NOT EXISTS dwi.dwi_res_regn_mergelocation_msk_d (
    mdn string comment '手机号码'  
    ,start_time string comment '业务开始时间'  
    ,county_id string comment '区县编码'  
    ,longi string comment '经度'  
    ,lati string comment '纬度'  
    ,bsid string comment '基站标识'  
    ,grid_id string comment '网格号'  
    ,biz_type string comment '业务类型'  
    ,event_type string comment '事件类型'  
    ,data_source string comment '数据源'  
) 
comment  '位置数据融合表'
PARTITIONED BY (
    day_id string comment '天分区'  
) 
ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\t' 
STORED AS PARQUET
location '/daas/motl/dwi/dwi_res_regn_mergelocation_msk_d'; 




alter table dwi.dwi_res_regn_mergelocation_msk_d  add if not exists partition(day_id='$day_id') location '/daas/motl/dwi/dwi_res_regn_mergelocation_msk_d/day_id=$day_id';