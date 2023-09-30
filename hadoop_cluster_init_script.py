#!/user/bin/env python
# coding:utf-8

"""
Hadoop 集群初始化脚本
"""

import os
import time
import socket
import tarfile
import threading
from invoke import Responder
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

jdk_file_name = 'jdk-8u212-linux-x64.tar.gz'
hadoop_file_name = 'hadoop-3.1.3.tar.gz'

# 初始化机器运行环境
for host in hosts:
    conn = Connection(host['dhcp_ip'], user=host['root_user'], connect_kwargs={'password': host['root_password']})

    # 安装基础软件
    conn.run('yum install -y vim rsync lrzsz wget ntp')

    # 关闭防火墙
    conn.run('systemctl stop firewalld && systemctl disable firewalld.service')

    # 修改主机名
    conn.run(f"echo '{host['hostname']}' > /etc/hostname")

    # 添加 hosts 配置
    for index, line in enumerate(host['hosts']):
        if index == 0:
            conn.run(f"echo >> /etc/hosts")
        conn.run(f"echo '{line['static_ip']} {line['hostname']}' >> /etc/hosts")

    # 修改网络配置
    ipa = conn.run("ip a | grep -E \"^[0-9]+: \" | awk -F': ' '$2~/^e/{print $2}' | sed -n '1p'")
    ifcfg_name = ipa.stdout.strip()
    network_ifcfg_path = f"/etc/sysconfig/network-scripts/ifcfg-{ifcfg_name}"

    conn.run(f"sed -i -e 's/BOOTPROTO.*/BOOTPROTO=\"static\"/' -e 's/ONBOOT.*/ONBOOT=\"yes\"/' {network_ifcfg_path}")

    conn.run(f"""
    echo "IPADDR={host['static_ip']}" >> {network_ifcfg_path}
    echo "NETMASK=255.255.255.0" >> {network_ifcfg_path}
    echo "GATEWAY={host['gateway']}" >> {network_ifcfg_path}
    echo "DNS1=8.8.8.8" >> {network_ifcfg_path}
    echo "DNS2=8.8.4.4" >> {network_ifcfg_path}
    """)

    # 创建工作目录
    conn.run('mkdir -p /opt/module && mkdir -p /opt/software')

    # 创建普通用户
    conn.run(f"""
    useradd {host['general_user']}
    echo "{host['general_user']}:{host['general_password']}" | chpasswd
    sed -i "/^%wheel/a {host['general_user']}    ALL=(ALL)       NOPASSWD:ALL" /etc/sudoers
    """)

    # 更改工作目录所属用户
    conn.run(f"""
    chown -R {host['general_user']}:{host['general_user']} /opt/module
    chown -R {host['general_user']}:{host['general_user']} /opt/software
    """)

    # 重启
    conn.run('reboot', disown=True)

    conn.close()


def check_connectivity(ip):
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # 创建一个TCP套接字
            # 设置连接超时时间为1秒
            sock.settimeout(1)
            # 尝试连接目标IP的某个端口（这里选择80端口作为示例）
            sock.connect((ip, 22))
            # 连接成功，输出提示信息并结束脚本
            print(f"IP {ip} is reachable.")
            break
        except socket.error as e:
            # 连接失败，输出错误信息，并等待1秒后再进行下一次尝试
            print(f"Connection to IP {ip} failed: {str(e)}")
            time.sleep(1)
        finally:
            sock.close()


def get_tar_gz_dir_name(tar_gz_file):
    with tarfile.open(tar_gz_file, "r:gz") as f:
        return f.getnames()[0]


# 上传脚本
for host in hosts:
    # 启动线程，等待条件满足
    t = threading.Thread(target=check_connectivity, args=(host['static_ip'],))
    t.start()

    # 阻塞，直到条件满足
    t.join()

    # 条件满足，执行操作
    conn = Connection(host['static_ip'], user=host['root_user'],
                      connect_kwargs={'password': host['root_password']})

    # 上传 xsync
    xsync_path = os.path.join(os.path.dirname(__file__), 'sh-scripts', 'xsync')
    conn.put(xsync_path, '/bin')
    conn.run('chmod +x /bin/xsync')

    # 上传 jpsall
    jpsall_path = os.path.join(os.path.dirname(__file__), 'sh-scripts', 'jpsall')
    conn.put(jpsall_path, '/bin')
    conn.run('chmod +x /bin/jpsall')

    # 上传 hadoop_cluster.sh
    myhadoop_path = os.path.join(os.path.dirname(__file__), 'sh-scripts', 'hadoop_cluster.sh')
    conn.put(myhadoop_path, '/bin')
    conn.run('chmod +x /bin/hadoop_cluster.sh')

    # 上传 xcall
    xcall_path = os.path.join(os.path.dirname(__file__), 'sh-scripts', 'xcall')
    conn.put(xcall_path, '/bin')
    conn.run('chmod +x /bin/xcall')

    conn.close()

# 构建 Hadoop 集群
for host in hosts:
    # 启动线程，等待条件满足
    t = threading.Thread(target=check_connectivity, args=(host['static_ip'],))
    t.start()

    # 阻塞，直到条件满足
    t.join()

    # 条件满足，执行操作
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

    my_env_path = '/etc/profile.d/my_env.sh'

    # 安装 JDK
    jdk_path = os.path.join(os.path.dirname(__file__), 'packages', jdk_file_name)
    conn.put(jdk_path, '/opt/software')
    conn.sudo(f"tar -zxvf /opt/software/{jdk_file_name} -C /opt/module/")

    # 添加 JAVA_HOME 环境变量
    jdk_dir_name = get_tar_gz_dir_name(jdk_path)

    conn.run(f"""
    sudo bash -c "echo >> {my_env_path}"
    sudo bash -c "echo '# JAVA_HOME' >> {my_env_path}"
    sudo bash -c "echo 'export JAVA_HOME=/opt/module/{jdk_dir_name}' >> {my_env_path}"
    sudo bash -c "echo 'export PATH=\\$PATH:\\$JAVA_HOME/bin' >> {my_env_path}"
    """)

    conn.run('source /etc/profile')

    # 安装 hadoop
    hadoop_path = os.path.join(os.path.dirname(__file__), 'packages', hadoop_file_name)
    conn.put(hadoop_path, '/opt/software')
    conn.sudo(f"tar -zxvf /opt/software/{hadoop_file_name} -C /opt/module/")

    # 添加 Hadoop 环境变量
    hadoop_dir_name = get_tar_gz_dir_name(hadoop_path)

    conn.run(f"""
    sudo bash -c "echo >> {my_env_path}"
    sudo bash -c "echo '# HADOOP_HOME' >> {my_env_path}"
    sudo bash -c "echo 'export HADOOP_HOME=/opt/module/{hadoop_dir_name}' >> {my_env_path}"
    sudo bash -c "echo 'export PATH=\\$PATH:\\$HADOOP_HOME/bin' >> {my_env_path}"
    sudo bash -c "echo 'export PATH=\\$PATH:\\$HADOOP_HOME/sbin' >> {my_env_path}"
    """)

    conn.run('source /etc/profile')

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
        <value>/opt/module/{hadoop_dir_name}/data</value>
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

    # SSH 免密登录配置
    key_pass = Responder(
        pattern=r'Enter file in which to save the key',
        response='\n',
    )
    passphrase_pass = Responder(
        pattern=r'Enter passphrase',
        response='\n',
    )
    same_passphrase_pass = Responder(
        pattern=r'Enter same passphrase again',
        response='\n',
    )

    conn.run('ssh-keygen -t rsa', pty=True, watchers=[key_pass, passphrase_pass, same_passphrase_pass])

    conn.close()

# 批量拷贝公钥
for host in hosts:
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

    continue_connecting_pass = Responder(
        pattern=r'\(yes/no\)',
        response='yes\n',
    )

    general_password_pass = Responder(
        pattern=r'password',
        response='{}\n'.format(host['general_password']),
    )

    for line in host['hosts']:
        conn.run(f"ssh-copy-id {line['hostname']}", pty=True,
                 watchers=[continue_connecting_pass, general_password_pass])

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

        conn.close()

    if host['hostname'] == 'hadoop102':
        conn = Connection(host['static_ip'], user=host['general_user'],
                          connect_kwargs={'password': host['general_password']})
        # 启动 yarn
        conn.run('$HADOOP_HOME/sbin/start-yarn.sh')

        conn.close()
