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

# zk 目录地址
zk_path="/opt/module/zookeeper"

# 判断参数个数和参数格式 (start,stop,restart,status)
if [[ $# != 1 ]] || [[ ! "$argument" =~ (start|stop|restart|status) ]]; then
  echo
  echo -e "\e[91m Parameter input error! The parameters can only be start, stop, restart, status! \e[0m"
  echo
  exit 1
fi

# 如果自定义的 hosts 变量为空, 那就从 /etc/hosts 文件中获取集群 hosts
hosts=$(echo $customize_hosts | tr ',' ' ')

if [[ $(echo $hosts | wc -w) == 0 ]]; then
  hosts=$(cat /etc/hosts | awk '$2!~/localhost/&&NF!=0{print $2}' | xargs)
fi

# 获取当前登录用户
user=`whoami`

# 遍历集群并执行命令
case $argument in
  "start") {
    echo -e "\e[92m ============================== 启动 Zookeeper 集群 ============================== \e[0m"
    for host in $hosts; do
      echo "--------------- 启动 $host ---------------"
      ssh $user@$host "$zk_path/bin/zkServer.sh start"
    done
  }
  ;;

  "stop") {
    echo -e "\e[91m ============================== 停止 Zookeeper 集群 ============================== \e[0m"
    for host in $hosts; do
      echo "--------------- 停止 $host ---------------"
      ssh $user@$host "$zk_path/bin/zkServer.sh stop"
    done
  }
  ;;

  "restart") {
    echo -e "\e[93m ============================== 重启 Zookeeper 集群 ============================== \e[0m"
    for host in $hosts; do
      echo "--------------- 重启 => 正在停止 => $host ---------------"
      ssh $user@$host "$zk_path/bin/zkServer.sh start"
    done

    echo
    echo " --------------- 等待 5s ---------------"
    sleep 5s
    echo

    for host in $hosts; do
      echo "--------------- 重启 => 正在启动 => $host ---------------"
      ssh $user@$host "$zk_path/bin/zkServer.sh stop"
    done
  };;

  "status") {
    echo -e "\e[92m ============================== 查看 Zookeeper 集群状态 ============================== \e[0m"
    for host in $hosts; do
      echo "--------------- 查看 $host 状态 ---------------"
      ssh $user@$host "$zk_path/bin/zkServer.sh status"
    done
  }
  ;;
esac