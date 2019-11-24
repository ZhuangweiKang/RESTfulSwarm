# -------------HTB------------------
# Client, Discovery
tc qdisc add dev xxx handle 1: root htb
tc class add dev xxx parent 1: classid 1:1 htb rate 1000mbps
tc qdisc add dev xxx parent 1:1 handle 10: netem delay xxxms xxms distribution normal
tc filter add dev xxx protocol tcp parent 1:0 prio 1 u32 match tcp dport xxxx flowid 1:1

tc qdisc del dev xxx handle 1: root

# FE, JM
tc qdisc add dev xxx handle 1: root htb
tc class add dev xxx parent 1: classid 1:1 htb
tc class add dev xxx parent 1: classid 1:2 htb
tc qdisc add dev xxx parent 1:1 handle 10: netem delay xxxms xxms distribution normal
tc qdisc add dev xxx parent 1:2 handle 20: netem delay xxxms xxms distribution normal
tc filter add dev xxx protocol tcp parent 1:0 prio 2 u32 match tcp dport xxxx flowid 1:1
tc filter add dev xxx protocol tcp parent 1:0 prio 1 u32 match tcp dport xxxx flowid 1:2

tc qdisc del dev xxx handle 1: root

# GM, Worker
tc qdisc add dev xxx handle 1: root htb
tc class add dev xxx parent 1: classid 1:1 htb rate
tc class add dev xxx parent 1: classid 1:2 htb rate
tc qdisc add dev xxx parent 1:1 handle 10: netem delay xxxms xxms distribution normal
tc qdisc add dev xxx parent 1:2 handle 20: netem delay xxxms xxms distribution normal
tc filter add dev xxx protocol ip parent 1:0 prio 2 u32 match ip dport xxxx flowid 1:1
tc filter add dev xxx protocol tcp parent 1:0 prio 1 u32 match tcp dport xxxx flowid 1:2

tc qdisc del dev xxx handle 1: root
