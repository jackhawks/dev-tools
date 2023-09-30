#!/bin/bash

#=======================================================================================================================
#	Zookeeper 集群 启动/停止/重启/查看状态 脚本
#
#	Version: 1.0.0
#	Author: Jack
#=======================================================================================================================

# 自定义 hosts (例如: 'hadoop101,hadoop102,hadoop103')
customize_hosts=''

# 参数
argument=$1

# 判断参数个数和参数格式 (start,stop,restart)
if [[ $# != 1 ]] || [[ ! "$argument" =~ (start|stop|restart) ]]; then
  echo
  echo -e "\e[91m Parameter input error! The parameters can only be start, stop, restart! \e[0m"
  echo
  exit 1
fi

# 如果自定义的 hosts 变量为空, 那就从 /etc/hosts 文件中获取集群 hosts
hosts=$(echo $customize_hosts | tr ',' ' ')

if [[ $(echo $hosts | wc -w) == 0 ]]; then
  hosts=$(cat /etc/hosts | awk '$2!~/localhost/&&NF!=0{print $2}' | xargs)
fi

hdfs_in_host='hadoop101'
yarn_in_host='hadoop102'
history_server_in_host='hadoop101'

# 遍历集群并执行命令
case $1 in
  "start") {
    echo -e "\e[92m ============================== 启动 hadoop集群 ============================== \e[0m"
    echo " --------------- 启动 hdfs ---------------"
    ssh $hdfs_in_host "$HADOOP_HOME/sbin/start-dfs.sh"

    echo " --------------- 启动 yarn ---------------"
    ssh $yarn_in_host "$HADOOP_HOME/sbin/start-yarn.sh"

    echo " --------------- 启动 history server ---------------"
    ssh $history_server_in_host "$HADOOP_HOME/bin/mapred --daemon start historyserver"
  }
  ;;

  "stop") {
    echo -e "\e[93m ============================== 关闭 hadoop集群 ============================== \e[0m"
    echo " --------------- 关闭 history server ---------------"
    ssh $history_server_in_host "$HADOOP_HOME/bin/mapred --daemon stop historyserver"

    echo " --------------- 关闭 yarn ---------------"
    ssh $yarn_in_host "$HADOOP_HOME/sbin/stop-yarn.sh"

    echo " --------------- 关闭 hdfs ---------------"
    ssh $hdfs_in_host "$HADOOP_HOME/sbin/stop-dfs.sh"
  }
  ;;

  *) {
    echo
    echo -e "\e[91m Not enough parameters! \e[0m"
    echo
  }
  ;;
esac
