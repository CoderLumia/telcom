#!/bin/bash

#获取脚本所在目录
shell_home="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $shell_home
day_id=$1
#spark-submit 提交任务
spark-submit --class com.lumia.merge.MergeLocation --master yarn --deploy-mode client --num-executors 2 --executor-cores 2 --executor-memory 2G --jars ../lib/common-1.0.jar  ../lib/mergelocation-1.0.jar  $day_id


#为hive表增加分区
hive-e "
alter table dwi.dwi_res_regn_mergelocation_msk_d add if not exists partition(day_id='$day_id') location '/daas/motl/dwi/dwi_res_regn_mergelocation_msk_d/day_id=$day_id';
"


