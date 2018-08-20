#!/bin/bash

source /etc/profile
cd /home/hadoop/deploy/spider/videowebset/
date >> nohup.out
today=`date -d "a day ago" +%Y-%m-%d`
echo $today" 补全用户观看URL的3个字段信息开始..." >> nohup.out

nohup python videowebset/run.py &
