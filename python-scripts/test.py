#!/user/bin/env python
# coding:utf-8

import os
from fabric import Connection

"""JDK 集群环境批量安装脚本
@Version    :   1.0.0
@Author     :   Jack

基础初始化集群中的所有机器

1.安装JDK
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

]

for host in hosts:
    # 建立连接
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

    # 修改 env 环境变量文件
    my_env_path = '/etc/profile.d/my_env.sh'

    java_env = f"""
    # JAVA_HOME
    export JAVA_HOME=/opt/module/jdk
    export PATH=\\$PATH:\\$JAVA_HOME/bin\
    """.replace('    ', '')

    conn.run(f"sudo bash -c 'echo \"{java_env}\" >> {my_env_path}'")

    conn.close()
