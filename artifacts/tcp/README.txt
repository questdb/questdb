sudo sysctl -w net.ipv4.tcp_max_syn_backlog="65536"
sudo sysctl -w net.ipv4.tcp_synack_retries="10"
sudo sysctl -w net.ipv4.tcp_max_orphans="1600000"
sudo sysctl -w net.ipv4.route.flush=1

sudo sysctl -w net.ipv4.tcp_syncookies=1


http://www.linux-admins.net/2010/09/linux-tcp-tuning.html

