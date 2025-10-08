#!/bin/bash

INTERFACE="wlan0"
MONITOR_TIME=10

echo "Monitoring ARP on $INTERFACE for $MONITOR_TIME seconds..."

sudo timeout $MONITOR_TIME tcpdump -i $INTERFACE -n arp 2>/dev/null | \
while read line; do
    if echo "$line" | grep -q "ARP, Request who-has"; then
        IP=$(echo "$line" | awk '{print $7}' | sed 's/,$//')

        if [ -n "$IP" ]; then
            echo "Found ARP request for $IP - checking if it responded..."
        fi
    fi
done

echo "Analyzing results..."
sudo timeout $MONITOR_TIME tcpdump -i $INTERFACE -n arp 2>/dev/null > /tmp/arp_dump.txt

grep "ARP, Request who-has" /tmp/arp_dump.txt | awk '{print $7}' | sed 's/,$//' | sort -u | \
while read IP; do
    if ! grep -q "ARP, Reply $IP is-at" /tmp/arp_dump.txt; then
        echo "=== FOUND NON-RESPONDING IP: $IP ==="
        echo "Pinging $IP to wake it up"
        ping -c 10 $IP
        rm -f /tmp/arp_dump.txt
        exit 0
    fi
done

echo "No non-responding ARP addresses found"
rm -f /tmp/arp_dump.txt