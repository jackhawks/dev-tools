#!/user/bin/env python
# coding:utf-8

import os

from fabric import Connection

"""ZK 集群环境批量安装脚本
@Version    :   1.0.0
@Author     :   Jack
"""

hosts = [
    {
        'dhcp_ip': '192.168.10.134',
        'root_user': 'root',
        'root_password': '123456',
        'general_user': 'jack',
        'general_password': '123456',
        'hostname': 'hadoop101',
        'static_ip': '192.168.10.101',
        'gateway': '192.168.10.2',
        'hosts': [
            {
                'static_ip': '192.168.10.101',
                'hostname': 'hadoop101',
            },
            {
                'static_ip': '192.168.10.102',
                'hostname': 'hadoop102',
            },
            {
                'static_ip': '192.168.10.103',
                'hostname': 'hadoop103',
            },
        ],
    },
    {
        'dhcp_ip': '192.168.10.135',
        'root_user': 'root',
        'root_password': '123456',
        'general_user': 'jack',
        'general_password': '123456',
        'hostname': 'hadoop102',
        'static_ip': '192.168.10.102',
        'gateway': '192.168.10.2',
        'hosts': [
            {
                'static_ip': '192.168.10.101',
                'hostname': 'hadoop101',
            },
            {
                'static_ip': '192.168.10.102',
                'hostname': 'hadoop102',
            },
            {
                'static_ip': '192.168.10.103',
                'hostname': 'hadoop103',
            },
        ],
    },
    {
        'dhcp_ip': '192.168.10.136',
        'root_user': 'root',
        'root_password': '123456',
        'general_user': 'jack',
        'general_password': '123456',
        'hostname': 'hadoop103',
        'static_ip': '192.168.10.103',
        'gateway': '192.168.10.2',
        'hosts': [
            {
                'static_ip': '192.168.10.101',
                'hostname': 'hadoop101',
            },
            {
                'static_ip': '192.168.10.102',
                'hostname': 'hadoop102',
            },
            {
                'static_ip': '192.168.10.103',
                'hostname': 'hadoop103',
            },
        ],
    },
]

for index, host in enumerate(hosts):
    # 建立连接
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

    # zk 下载地址
    zk_download_url = 'https://github.com/jackhawks/dev-helper/releases/download/v1.0.0/apache-zookeeper-3.5.7-bin.tar.gz'

    # 下载 zk 到 /opt/software 目录下
    conn.sudo(f"wget -P /opt/software {zk_download_url}")

    # 解压 zk 压缩包到 /opt/module 目录下
    zk_file_name = os.path.basename(zk_download_url)
    conn.sudo('mkdir -p /opt/module/zookeeper')
    conn.sudo(f"chown -R {host['general_user']}:{host['general_user']} /opt/module/zookeeper")
    conn.run(f"tar -zxvf /opt/software/{zk_file_name} --strip-component=1 -C /opt/module/zookeeper")

    # 创建 zkData 目录, 添加 myid 文件
    conn.run('mkdir -p /opt/module/zookeeper/zkData')
    conn.run(f"echo {index + 1} > /opt/module/zookeeper/zkData/myid")

    # 重命名 zoo.cfg 文件
    conf = '/opt/module/zookeeper/conf'
    conn.run(f"mv {conf}/zoo_sample.cfg {conf}/zoo.cfg")

    # 修改数据存储路径配置
    conn.run(f"sed -i 's#dataDir=.*#dataDir=/opt/module/zookeeper/zkData#' {conf}/zoo.cfg")

    # 添加集群配置
    conn.run(f"echo '####################### zookeeper cluster ##########################' >> {conf}/zoo.cfg")
    for idx, line in enumerate(host['hosts']):
        conn.run(f"echo 'server.{idx + 1}={line['hostname']}:2888:3888' >> {conf}/zoo.cfg")

    # 关闭连接
    conn.close()

# 启动 zk 集群
for index, host in enumerate(hosts):
    # 建立连接
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

    # 启动 zk
    conn.run('/opt/module/zookeeper/bin/zkServer.sh start')

    # 关闭连接
    conn.close()

# 上传基础脚本
for host in hosts:
    conn = Connection(host['static_ip'], user=host['root_user'],
                      connect_kwargs={'password': host['root_password']})

    # 上传 zk 集群脚本
    zk_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'shell-scripts', 'bigdata',
                           'zk_cluster.sh')
    conn.put(zk_path, '/bin')
    conn.run('chmod +x /bin/zk_cluster.sh')

    # 关闭连接
    conn.close()
