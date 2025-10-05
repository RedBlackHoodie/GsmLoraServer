#!/bin/bash

INTERFACE="wlan0"
MONITOR_TIME=30
PCAP_FILE="/tmp/arp_capture.pcap"

echo "ARP scan on $INTERFACE for $MONITOR_TIME seconds..."

sudo timeout $MONITOR_TIME tcpdump -i $INTERFACE -w $PCAP_FILE 'arp' 2>/dev/null &
TCPDUMP_PID=$!

wait $TCPDUMP_PID

IP_LIST=$(sudo tcpdump -r $PCAP_FILE 'arp' 2>/dev/null | grep "ARP, Request" | awk -F' ' '{print $7}' | sort -u)

for IP in $IP_LIST; do
    echo "Testing $IP with ARP..."
    if ! sudo arping -c 2 -I $INTERFACE $IP 2>/dev/null | grep -q "Reply"; then
        echo "Address not responding to ARP: $IP"
        echo "Starting ping to $IP"
        ping $IP
        sudo rm -f $PCAP_FILE
        exit 0
    fi
done

echo "No non-responding ARP addresses found"
sudo rm -f $PCAP_FILE