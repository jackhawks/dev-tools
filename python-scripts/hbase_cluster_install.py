#!/user/bin/env python
# coding:utf-8

import os

from fabric import Connection

"""HBase 集群环境批量安装脚本
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

    # hbase 下载地址
    hbase_download_url = 'https://github.com/jackhawks/dev-helper/releases/download/v1.0.0/hbase-2.4.11-bin.tar.gz'

    # 下载 hbase 到 /opt/software 目录下
    conn.sudo(f"wget -P /opt/software {hbase_download_url}")

    # 解压 hbase 压缩包到 /opt/module 目录下
    hbase_file_name = os.path.basename(hbase_download_url)
    conn.sudo('mkdir -p /opt/module/hbase')
    conn.sudo(f"chown -R {host['general_user']}:{host['general_user']} /opt/module/hbase")
    conn.run(f"tar -zxvf /opt/software/{hbase_file_name} --strip-component=1 -C /opt/module/hbase")

    # 修改 env 环境变量文件
    my_env_path = '/etc/profile.d/my_env.sh'

    hbase_env = f"""\
    # HBASE_HOME
    export JAVA_HOME=/opt/module/hbase
    export PATH=\\$PATH:\\$HBASE_HOME/bin\
    """.replace('    ', '')

    conn.run(f"sudo bash -c 'echo \"{hbase_env}\" >> {my_env_path}'")

    # 使环境变量配置生效
    conn.run('source /etc/profile')

    # 修改 hbase-env.sh
    conn.run(f"sed -i 's/# export HBASE_MANAGES_ZK=true/export HBASE_MANAGES_ZK=true/' $HBASE_HOME/conf/hbase-env.sh")

    # 修改 hbase-site.xml
    conn.run("sed -i '/<configuration>/, /<\/configuration>/d' $HBASE_HOME/conf/hbase-site.xml")

    hosts_array = []
    for line in host['hosts']:
        hosts_array.append(line['hostname'])

    hbase_site = f"""
    <configuration>
        <property>
            <name>hbase.zookeeper.quorum</name>
            <value>{','.join(hosts_array)}</value>
        </property>
        
        <property>
            <name>hbase.rootdir</name>
            <value>hdfs://{hosts_array[0]}:8020/hbase</value>
        </property>
        
        <property>
            <name>hbase.cluster.distributed</name>
            <value>true</value>
        </property>
    </configuration>
    """.replace('\n', '\\n')
    conn.run(f"sed -i '-->/a\\{hbase_site}' $HBASE_HOME/conf/hbase-site.xml")

    regionservers = '\n'.join(hosts_array)

    conn.run(f"sudo bash -c 'echo \"{regionservers}\" > $HBASE_HOME/conf/regionservers'")

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
