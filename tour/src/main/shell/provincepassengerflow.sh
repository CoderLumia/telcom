#!/bin/bash

#获取脚本所在目录
shell_home="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $shell_home
day_id=$1
month_id=$2

# spark-submit 提交任务
spark-submit --class com.lumia.tour.statistics.ProvincePassengerFlow --master yarn --deploy-mode client --num-executors 2 --executor-cores 2 --executor-memory 2G --jars ../lib/common-1.0.jar,../lib/jedis-2.9.1.jar  ../lib/tour-1.0.jar  $day_id $month_id