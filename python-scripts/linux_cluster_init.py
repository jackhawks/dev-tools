#!/user/bin/env python
# coding:utf-8

"""Linux 集群初始化脚本
@Version    :   1.0.0
@Author     :   Jack

初始化集群中的所有机器

1.安装基础软件
2.关闭防火墙
3.修改主机名
4.添加 hosts 映射
5.配置静态 Ip
6.创建工作目录
7.创建普通用户
8.配置 SSH 免密登录
9.上传基础脚本
"""

import os
import time
import socket
import threading
from invoke import Responder
from fabric import Connection

hosts = [
    {
        'dhcp_ip': '192.168.10.133',
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
        'dhcp_ip': '192.168.10.137',
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
        'dhcp_ip': '192.168.10.138',
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
    conn = Connection(host['dhcp_ip'], user=host['root_user'], connect_kwargs={'password': host['root_password']})

    # 安装基础软件
    conn.run('yum install -y vim rsync lrzsz wget dnf')

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

    # 关闭连接
    conn.close()


def check_connectivity(ip):
    """检查连接性"""

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


# SSH 免密登录配置-密钥生成
for host in hosts:
    # 启动线程，等待条件满足
    t = threading.Thread(target=check_connectivity, args=(host['static_ip'],))
    t.start()

    # 阻塞，直到条件满足
    t.join()

    # 条件满足，执行操作
    conn = Connection(host['static_ip'], user=host['general_user'],
                      connect_kwargs={'password': host['general_password']})

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

    # 关闭连接
    conn.close()

# SSH 免密登录配置-密钥拷贝
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

    # 关闭连接
    conn.close()

# 上传基础脚本
for host in hosts:
    conn = Connection(host['static_ip'], user=host['root_user'],
                      connect_kwargs={'password': host['root_password']})

    # 上传 jps all
    jps_all_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'shell-scripts', 'basis', 'jpsall')
    conn.put(jps_all_path, '/bin')
    conn.run('chmod +x /bin/jpsall')

    # 上传 xcall
    xcall_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'shell-scripts', 'basis', 'xcall')
    conn.put(xcall_path, '/bin')
    conn.run('chmod +x /bin/xcall')

    # 上传 xsync
    xsync_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'shell-scripts', 'basis', 'xsync')
    conn.put(xsync_path, '/bin')
    conn.run('chmod +x /bin/xsync')

    # 关闭连接
    conn.close()
