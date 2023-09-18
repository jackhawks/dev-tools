#!/bin/bash

# 从 /etc/hosts 文件获取集群 host
hosts=$(cat /etc/hosts | awk '$2!~/localhost/&&NF!=0{print $2}' | xargs)

hdfs_in_host='hadoop101'
yarn_in_host='hadoop102'
history_server_in_host='hadoop101'

case $1 in
"start")
  echo -e "\e[92m ============================== 启动 hadoop集群 ============================== \e[0m"

  echo " --------------- 启动 hdfs ---------------"
  ssh $hdfs_in_host "$HADOOP_HOME/sbin/start-dfs.sh"

  echo " --------------- 启动 yarn ---------------"
  ssh $yarn_in_host "$HADOOP_HOME/sbin/start-yarn.sh"

  echo " --------------- 启动 history server ---------------"
  ssh $history_server_in_host "$HADOOP_HOME/bin/mapred --daemon start historyserver"
;;
"stop")
  echo -e "\e[93m ============================== 关闭 hadoop集群 ============================== \e[0m"

  echo " --------------- 关闭 history server ---------------"
  ssh $history_server_in_host "$HADOOP_HOME/bin/mapred --daemon stop historyserver"

  echo " --------------- 关闭 yarn ---------------"
  ssh $yarn_in_host "$HADOOP_HOME/sbin/stop-yarn.sh"

  echo " --------------- 关闭 hdfs ---------------"
  ssh $hdfs_in_host "$HADOOP_HOME/sbin/stop-dfs.sh"
;;
*)
  echo
  echo -e "\e[91m Not enough parameters! \e[0m"
  echo
;;
esac
