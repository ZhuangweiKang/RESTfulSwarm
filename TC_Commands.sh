# 排队规则 qdisc(queueing discipline )
# Class类，用来表示不同的流量控制策略
# Filter规则，用来将用户（IP）划入具体的控制策略（class）中， filter 划分的标志位可用 u32 打标功能或 IPtables 的 set-mark (大多使用iptables 来做标记)功能来实现。

# TC 的应用流程： 数据包->iptables(在通过 iptables 时,iptables 根据不同的 ip 来设置不同的 mark)->TC(class)->TC(queue)

# --------------------------------------------------------------------------------------

tc qdisc add dev em1 root handle 1: prio
tc qdisc add dev em1 parent 1:3 handle 30: \
netem  delay 30ms 6ms distribution normal
tc filter add dev em1 protocol ip parent 1:0 prio 3 u32 \
     match ip dst 129.59.105.37/32 flowid 1:3
     
     
tc filter del dev em1 protocol ip parent 1:0 prio 3 u32
tc qdisc del dev em1 parent 1:3 handle 30:

tc qdisc add dev em1 parent 1:3 handle 30: \
netem  delay 30ms 5ms distribution normal

tc qdisc add dev em1 parent 1:3 handle 30: \
netem  delay 1ms 0.5ms distribution normal

tc filter add dev em1 protocol ip parent 1:0 prio 3 u32 \
     match ip dst 129.59.234.136/32 flowid 1:3


tc filter add dev em1 protocol ip parent 1:0 prio 3 u32 \
     match ip dst 129.59.234.251/32 flowid 1:3
     
     
tc qdisc add dev em1 parent 1:3 handle 30: \
netem  delay 40ms 8ms distribution normal
tc qdisc change dev em1 parent 1:3 handle 30: \
netem  delay 20ms 4ms distribution normal
tc qdisc add dev em0 root handle 1: prio
tc qdisc add dev em0 parent 1:3 handle 30: \
netem  delay 5ms 1ms distribution normal
tc filter change dev em0 protocol ip parent 1:0 prio 3 u32 \
     match ip dst 129.59.105.87/32 flowid 1:3

tc qdisc change dev em0 parent 1:3 handle 30: netem  delay 10ms 2ms distribution normal


tc qdisc add dev em0 root handle 1: prio
tc qdisc change dev em0 parent 1:3 handle 30: \
netem  delay 9ms 1.8ms distribution normal
tc filter add dev em0 protocol ip parent 1:0 prio 3 u32 \
     match ip dst 129.59.234.136/32 flowid 1:3
     
     
tc qdisc add dev em0 parent 1:3 handle 31: netem  delay 5ms 1ms distribution normal
tc filter add dev em0 protocol ip parent 1:0 prio 4 u32 match ip dst 129.59.234.249/32 flowid 1:4



tc qdisc add dev em0 handle 1: root htb
tc class add dev em0 parent 1: classid 1:1 htb rate 1000Mbps

tc class add dev em0 parent 1:1 classid 1:11 htb rate 1000Mbps
tc class add dev em0 parent 1:1 classid 1:12 htb rate 1000Mbps
tc class add dev em0 parent 1:1 classid 1:13 htb rate 1000Mbps
tc class add dev em0 parent 1:1 classid 1:14 htb rate 1000Mbps
tc class add dev em0 parent 1:1 classid 1:15 htb rate 1000Mbps

tc qdisc add dev em0 parent 1:11 handle 10: netem delay 50ms 10ms distribution normal
tc qdisc add dev em0 parent 1:12 handle 20: netem delay 40ms 8ms distribution normal
tc qdisc add dev em0 parent 1:13 handle 30: netem delay 30ms 6ms distribution normal
tc qdisc add dev em0 parent 1:14 handle 40: netem delay 20ms 4ms distribution normal
tc qdisc add dev em0 parent 1:15 handle 50: netem delay 5ms 1ms distribution normal


tc filter add dev em0 protocol ip prio 1 u32 match ip dst 129.59.234.221/32 flowid 1:11
tc filter add dev em0 protocol ip prio 1 u32 match ip dst 129.59.234.252/32 flowid 1:12
tc filter add dev em0 protocol ip prio 1 u32 match ip dst 129.59.234.248/32 flowid 1:13
tc filter add dev em0 protocol ip prio 1 u32 match ip dst 129.59.234.249/32 flowid 1:14
tc filter add dev em0 protocol ip prio 1 u32 match ip dst 129.59.234.207/32 flowid 1:15



tc class add dev em0 parent 1:1 classid 1:16 htb rate 1000Mbps
tc qdisc add dev em0 parent 1:16 handle 60: netem delay 20ms 4ms distribution normal
tc filter add dev em0 protocol ip prio 1 u32 match ip dst 129.59.105.87/32 flowid 1:16


tc qdisc add dev p1p1 handle 1: root htb
tc class add dev p1p1 parent 1:1 classid 1:11 htb rate 10Mbps
tc filter add dev p1p1 protocol ip prio 1 u32 match ip dst 129.59.105.87/32 flowid 1:11
