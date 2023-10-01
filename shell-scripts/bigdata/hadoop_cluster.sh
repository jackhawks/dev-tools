#!/bin/bash

#=======================================================================================================================
#	Hadoop 集群 启动/停止/重启 脚本
#
#	Version: 1.0.0
#	Author: Jack
#=======================================================================================================================

# 参数
argument=$1

# 判断参数个数和参数格式 (start,stop,restart)
if [[ $# != 1 ]] || [[ ! "$argument" =~ (start|stop|restart) ]]; then
  echo
  echo -e "\e[91m Parameter input error! The parameters can only be start, stop, restart! \e[0m"
  echo
  exit 1
fi

# 从配置文件获取配置项
hdfs_in_host=$(awk -F'[//:]' '/fs.defaultFS/{getline;print $4}' /opt/module/hadoop/etc/hadoop/core-site.xml)
yarn_in_host=$(awk -F'[><]' '/yarn.resourcemanager.hostname/{getline;print $3}' /opt/module/hadoop/etc/hadoop/yarn-site.xml)
history_server_in_host=$(awk -F'[>:]' '/mapreduce.jobhistory.address/{getline;print $2}' /opt/module/hadoop/etc/hadoop/mapred-site.xml)

# 解决特殊情况下配置不生效问题
source /etc/profile

# 遍历集群并执行命令
case $1 in
  "start") {
    echo -e "\e[92m ============================== 启动 hadoop 集群 ============================== \e[0m"
    echo " --------------- 启动 hdfs ---------------"
    ssh $hdfs_in_host "$HADOOP_HOME/sbin/start-dfs.sh"

    echo " --------------- 启动 yarn ---------------"
    ssh $yarn_in_host "$HADOOP_HOME/sbin/start-yarn.sh"

    echo " --------------- 启动 history server ---------------"
    ssh $history_server_in_host "$HADOOP_HOME/bin/mapred --daemon start historyserver"
  }
  ;;

  "stop") {
    echo -e "\e[91m ============================== 关闭 hadoop 集群 ============================== \e[0m"
    echo " --------------- 关闭 history server ---------------"
    ssh $history_server_in_host "$HADOOP_HOME/bin/mapred --daemon stop historyserver"

    echo " --------------- 关闭 yarn ---------------"
    ssh $yarn_in_host "$HADOOP_HOME/sbin/stop-yarn.sh"

    echo " --------------- 关闭 hdfs ---------------"
    ssh $hdfs_in_host "$HADOOP_HOME/sbin/stop-dfs.sh"
  }
  ;;

  "restart") {
    echo -e "\e[93m ============================== 重启 hadoop 集群 ============================== \e[0m"
    echo " --------------- 关闭 history server ---------------"
    ssh $history_server_in_host "$HADOOP_HOME/bin/mapred --daemon stop historyserver"

    echo " --------------- 关闭 yarn ---------------"
    ssh $yarn_in_host "$HADOOP_HOME/sbin/stop-yarn.sh"

    echo " --------------- 关闭 hdfs ---------------"
    ssh $hdfs_in_host "$HADOOP_HOME/sbin/stop-dfs.sh"

    echo
    echo " --------------- 等待 5s ---------------"
    sleep 5s
    echo

    echo " --------------- 启动 hdfs ---------------"
    ssh $hdfs_in_host "$HADOOP_HOME/sbin/start-dfs.sh"

    echo " --------------- 启动 yarn ---------------"
    ssh $yarn_in_host "$HADOOP_HOME/sbin/start-yarn.sh"

    echo " --------------- 启动 history server ---------------"
    ssh $history_server_in_host "$HADOOP_HOME/bin/mapred --daemon start historyserver"
  }
  ;;
esac
