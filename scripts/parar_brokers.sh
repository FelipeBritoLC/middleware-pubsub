#!/bin/bash
cd "$(dirname "$0")/.."

for i in 1 2 3; do
    if [ -f logs/broker$i.pid ]; then
        kill $(cat logs/broker$i.pid) 2>/dev/null
        rm logs/broker$i.pid
    fi
done
echo "brokers parados"
