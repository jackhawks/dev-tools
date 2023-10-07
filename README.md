# dev-helper



## 脚本关系图

<img src="./assets/2023-10-03-0310.png" style="zoom: 80%;width:80%" />

## 脚本介绍

|             脚本名称              | 简介                                                         | 备注                                                         |
| :-------------------------------: | ------------------------------------------------------------ | ------------------------------------------------------------ |
|              jpsall               | 查看集群的 jps 进程                                          | 可以自定义 hosts，详见脚本内容                               |
|               xsync               | 集群文件或目录同步脚本                                       | 可以自定义 hosts，详见脚本内容                               |
|               xcall               | 集群命令同步脚本，可以把命令同步到所有机器中执行             | 可以自定义 hosts，详见脚本内容                               |
|         hadoop_cluster.sh         | hadoop 集群操作脚本，可以 启动/停止/重启                     | ```start，stop，restart```                                   |
|           zk_cluster.sh           | Zookeeper 集群操作脚本， 可以 启动/停止/重启/查看状态        | ```start，stop，restart，status```                           |
|     hadoop_cluster_install.py     | 根据指定的配置，一键安装 hadoop 集群                         | 需要基于<br/>```jdk_cluster_install.py```                    |
|       zk_cluster_install.py       | 根据指定的配置，一键安装 zookeeper 集群                      | 需要基于<br/>```jdk_cluster_install.py```                    |
|      jdk_cluster_install.py       | 根据指定的配置，一键在集群中安装 Java 环境                   | 需要基于<br/>```linux_cluster_init.py```                     |
|        linux_basis_init.py        | Linux 机器基础初始化脚本，根据指定的配置初始化集群中的所有机器 | 1.安装基础软件<br/>2.关闭防火墙<br/>3.修改主机名<br/>4.添加 hosts 映射<br/>5.配置静态 Ip |
|       linux_cluster_init.py       | Linux 集群大数据环境基础初始化脚本，根据指定的配置初始化集群中的所有机器 | 1.安装基础软件<br/>2.关闭防火墙<br/>3.修改主机名<br/>4.添加 hosts 映射<br/>5.配置静态 Ip<br/>6.创建工作目录<br/>7.创建普通用户<br/>8.配置 SSH 免密登录<br/>9.上传基础脚本 |
| hbase_cluster_install.py | 根据指定的配置，一键安装 hbase 集群 | 需要基于<br/>```jdk_cluster_install.py```<br/>```zk_cluster.py``` |

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=jackhawks/dev-helper&type=Date)](https://star-history.com/#jackhawks/dev-helper&Date)
