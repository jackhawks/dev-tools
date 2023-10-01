#!/user/bin/env python
# coding:utf-8

"""Hadoop 集群安装脚本
@Version    :   1.0.0
@Author     :   Jack

安装 Hadoop 集群

版本 hadoop-3.1.3
"""

import os

from fabric import Connection

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

# 构建 Hadoop 集群
for host in hosts:
    # 创建连接
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

    # hadoop 下载地址
    hadoop_download_url = 'https://github.com/jackhawks/dev-helper/releases/download/v1.0.0/hadoop-3.1.3.tar.gz'

    # 下载 hadoop 到 /opt/software 目录下
    conn.sudo(f"wget -P /opt/software {hadoop_download_url}")

    # 解压 hadoop 压缩包到 /opt/module 目录下
    hadoop_file_name = os.path.basename(hadoop_download_url)
    conn.sudo('mkdir -p /opt/module/hadoop')
    conn.sudo(f"chown -R {host['general_user']}:{host['general_user']} /opt/module/hadoop")
    conn.run(f"tar -zxvf /opt/software/{hadoop_file_name} --strip-component=1 -C /opt/module/hadoop")

    # 修改 env 环境变量文件
    my_env_path = '/etc/profile.d/my_env.sh'

    hadoop_env = f"""\
    # HADOOP_HOME
    export HADOOP_HOME=/opt/module/hadoop
    export PATH=\\$PATH:\\$HADOOP_HOME/bin
    export PATH=\\$PATH:\\$HADOOP_HOME/sbin\
    """.replace('    ', '')

    conn.run(f"sudo bash -c 'echo \"{hadoop_env}\" >> {my_env_path}'")

    # 使环境变量配置生效
    conn.run('source /etc/profile')

    # 关闭连接
    conn.close()

# 修改 hadoop 集群配置
for host in hosts:
    # 创建连接
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

    name_node = host['hosts'][0]['hostname']
    name_node2 = host['hosts'][-1]['hostname']
    resource_manager = host['hosts'][1]['hostname']

    # 配置 hadoop core-site.xml
    core_site = f"""
    <!-- 指定 NameNode 的地址 -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://{name_node}:8020</value>
    </property>

    <!-- 指定 hadoop 数据的存储目录 -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/module/hadoop/data</value>
    </property>

    <!-- 配置 HDFS 网页登录使用的静态用户 -->
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>{host['general_user']}</value>
    </property>
    """.replace('\n', '\\n')

    conn.run(f"sed -i '/<configuration>/a\\{core_site}' $HADOOP_HOME/etc/hadoop/core-site.xml")

    # 配置 hadoop hdfs-site.xml
    hdfs_site = f"""
	<!-- NN web端访问地址 -->
	<property>
        <name>dfs.namenode.http-address</name>
        <value>{name_node}:9870</value>
    </property>

	<!-- 2NN web端访问地址 -->
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>{name_node2}:9868</value>
    </property>
    """.replace('\n', '\\n')

    conn.run(f"sed -i '/<configuration>/a\\{hdfs_site}' $HADOOP_HOME/etc/hadoop/hdfs-site.xml")

    # 配置 hadoop yarn-site.xml
    yarn_site = f"""
    <!-- 指定 Map Reduce 走 shuffle -->
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <!-- 指定 ResourceManager 地址-->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>{resource_manager}</value>
    </property>

    <!-- 环境变量的继承 -->
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>

    <!-- 开启日志聚集功能 -->
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>

    <!-- 设置日志聚集服务器地址 -->
    <property>  
        <name>yarn.log.server.url</name>  
        <value>http://{name_node}:19888/jobhistory/logs</value>
    </property>

    <!-- 设置日志保留时间为 7 天 -->
    <property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>604800</value>
    </property>
    """.replace('\n', '\\n')

    conn.run(f"sed -i '/<configuration>/a\\{yarn_site}' $HADOOP_HOME/etc/hadoop/yarn-site.xml")

    # 配置 mapred-site.xml
    mapred_site = f"""
    <!-- 指定 Map Reduce 程序运行在 Yarn 上 -->
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>

    <!-- 历史服务器端地址 -->
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>{name_node}:10020</value>
    </property>

    <!-- 历史服务器 web 端地址 -->
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>{name_node}:19888</value>
    </property>
    """.replace('\n', '\\n')

    conn.run(f"sed -i '/<configuration>/a\\{mapred_site}' $HADOOP_HOME/etc/hadoop/mapred-site.xml")

    # 配置 workers
    conn.run('> $HADOOP_HOME/etc/hadoop/workers')
    for host in host['hosts']:
        conn.run(f"echo '{host['hostname']}' >> $HADOOP_HOME/etc/hadoop/workers")

    # 关闭连接
    conn.close()

# 上传基础脚本
for host in hosts:
    conn = Connection(host['static_ip'], user=host['root_user'],
                      connect_kwargs={'password': host['root_password']})

    # 上传 hadoop 集群脚本
    hadoop_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'shell-scripts', 'bigdata',
                               'hadoop_cluster.sh')
    conn.put(hadoop_path, '/bin')
    conn.run('chmod +x /bin/hadoop_cluster.sh')

    # 关闭连接
    conn.close()

# 启动 Hadoop 集群
for host in hosts:

    if host['hostname'] == 'hadoop101':
        conn = Connection(host['static_ip'], user=host['general_user'],
                          connect_kwargs={'password': host['general_password']})

        # 第一次启动，需要初始化集群
        conn.run('hdfs namenode -format')

        # 启动 hdfs
        conn.run('$HADOOP_HOME/sbin/start-dfs.sh')

        # 启动服务器
        conn.run('mapred --daemon start historyserver')

        # 关闭连接
        conn.close()

    # 启动 yarn
    if host['hostname'] == 'hadoop102':
        conn = Connection(host['static_ip'], user=host['general_user'],
                          connect_kwargs={'password': host['general_password']})

        conn.run('$HADOOP_HOME/sbin/start-yarn.sh')

        # 关闭连接
        conn.close()
