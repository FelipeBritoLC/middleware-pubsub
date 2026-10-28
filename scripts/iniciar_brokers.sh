#!/bin/bash
set -e
cd "$(dirname "$0")/.."

mkdir -p bin logs
go build -o bin/ ./cmd/... ./exemplos/...

./bin/broker -endereco :7001 -pares localhost:7002,localhost:7003 > logs/broker1.log 2>&1 &
echo $! > logs/broker1.pid
./bin/broker -endereco :7002 -pares localhost:7001,localhost:7003 > logs/broker2.log 2>&1 &
echo $! > logs/broker2.pid
./bin/broker -endereco :7003 -pares localhost:7001,localhost:7002 > logs/broker3.log 2>&1 &
echo $! > logs/broker3.pid

echo "3 brokers rodando nas portas 7001, 7002 e 7003 (logs em logs/)"
echo "para parar: ./scripts/parar_brokers.sh"
