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

for host in hosts:
    # 建立连接
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

    # jdk 下载地址
    jdk_download_url = 'https://github.com/jackhawks/dev-helper/releases/download/v1.0.0/jdk-8u212-linux-x64.tar.gz'

    # 判断文件目录是否存在
    if conn.run('test -d /opt/software', warn=True).failed:
        conn.sudo('mkdir -p /opt/software')
        conn.sudo(f"chown -R {host['general_user']}:{host['general_user']} /opt/software")

    if conn.run('test -d /opt/module', warn=True).failed:
        conn.sudo('mkdir -p /opt/module')
        conn.sudo(f"chown -R {host['general_user']}:{host['general_user']} /opt/module")

    # 下载 jdk 到 /opt/software 目录下
    conn.sudo(f"wget -P /opt/software {jdk_download_url}")

    # 解压 jdk 压缩包到 /opt/module 目录下
    jdk_file_name = os.path.basename(jdk_download_url)
    conn.sudo('mkdir -p /opt/module/jdk')
    conn.sudo(f"chown -R {host['general_user']}:{host['general_user']} /opt/module/jdk")
    conn.run(f"tar -zxvf /opt/software/{jdk_file_name} --strip-component=1 -C /opt/module/jdk")

    # 修改 env 环境变量文件
    my_env_path = '/etc/profile.d/my_env.sh'

    java_env = f"""\
    # JAVA_HOME
    export JAVA_HOME=/opt/module/jdk
    export PATH=\\$PATH:\\$JAVA_HOME/bin\
    """.replace('    ', '')

    conn.run(f"sudo bash -c 'echo \"{java_env}\" >> {my_env_path}'")

    # 使环境变量配置生效
    conn.run('source /etc/profile')

    # 关闭连接
    conn.close()
