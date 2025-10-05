#!/bin/bash

INTERFACE="wlan0"
MONITOR_TIME=30
PCAP_FILE="/tmp/arp_capture.pcap"

echo "arp scan on $INTERFACE for $MONITOR_TIME s..."

timeout $MONITOR_TIME tcpdump -i $INTERFACE -w $PCAP_FILE 'arp' 2>/dev/null &
TCPDUMP_PID=$!

wait $TCPDUMP_PID

IP_LIST=$(tcpdump -r $PCAP_FILE 'arp' 2>/dev/null | grep "ARP, Request" | awk -F' ' '{print $7}' | sort -u)

for IP in $IP_LIST; do
    echo "resolve $IP..."
    if ! arping -c 2 -I $INTERFACE $IP 2>/dev/null | grep -q "Reply"; then
        echo "Adress not responding: $IP"
        echo "Start ping"
        ping $IP
        exit 0
    fi
done

echo "done"
rm -f $PCAP_FILE