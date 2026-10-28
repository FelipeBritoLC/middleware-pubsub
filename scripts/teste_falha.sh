#!/bin/bash
cd "$(dirname "$0")/.."

./scripts/iniciar_brokers.sh
sleep 1

echo "iniciando teste de carga (60s)..."
./bin/carga -duracao 60s -saida resultados/falha > logs/carga_falha.log 2>&1 &
CARGA=$!

sleep 20
echo "derrubando o broker 1 (porta 7001)..."
kill -9 $(cat logs/broker1.pid)

sleep 20
echo "religando o broker 1..."
./bin/broker -endereco :7001 -pares localhost:7002,localhost:7003 >> logs/broker1.log 2>&1 &
echo $! > logs/broker1.pid

wait $CARGA
cat resultados/falha/resumo.txt

./scripts/parar_brokers.sh
