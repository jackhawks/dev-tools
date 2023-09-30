#!/user/bin/env python
# coding:utf-8

"""Linux 机器基础初始化脚本
@Version    :   1.0.0
@Author     :   Jack

基础初始化集群中的所有机器

1.安装基础软件
2.关闭防火墙
3.修改主机名
4.添加 hosts 映射
5.配置静态 Ip
"""

from fabric import Connection

hosts = [
    {
        'dhcp_ip': '192.168.10.134',
        'root_user': 'root',
        'root_password': '123456',
        'hostname': 'hadoop101',
        'static_ip': '192.168.10.101',
        'gateway': '192.168.10.2',
        'hosts': [
            {
                'static_ip': '192.168.10.134',
                'hostname': 'hadoop101',
            },
            {
                'static_ip': '192.168.10.135',
                'hostname': 'hadoop102',
            },
            {
                'static_ip': '192.168.10.136',
                'hostname': 'hadoop103',
            },
        ],
    },
    {
        'dhcp_ip': '192.168.10.135',
        'root_user': 'root',
        'root_password': '123456',
        'hostname': 'hadoop102',
        'static_ip': '192.168.10.102',
        'gateway': '192.168.10.2',
        'hosts': [
            {
                'static_ip': '192.168.10.134',
                'hostname': 'hadoop101',
            },
            {
                'static_ip': '192.168.10.135',
                'hostname': 'hadoop102',
            },
            {
                'static_ip': '192.168.10.136',
                'hostname': 'hadoop103',
            },
        ],
    },
    {
        'dhcp_ip': '192.168.10.136',
        'root_user': 'root',
        'root_password': '123456',
        'hostname': 'hadoop103',
        'static_ip': '192.168.10.103',
        'gateway': '192.168.10.2',
        'hosts': [
            {
                'static_ip': '192.168.10.134',
                'hostname': 'hadoop101',
            },
            {
                'static_ip': '192.168.10.135',
                'hostname': 'hadoop102',
            },
            {
                'static_ip': '192.168.10.136',
                'hostname': 'hadoop103',
            },
        ],
    },
]

for host in hosts:
    conn = Connection(host['dhcp_ip'], user=host['root_user'], connect_kwargs={'password': host['root_password']})

    # 安装基础软件
    conn.run('yum install -y vim rsync lrzsz wget')

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

    # 重启
    conn.run('reboot', disown=True)

    # 关闭连接
    conn.close()