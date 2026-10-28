#!/bin/bash
cd "$(dirname "$0")/.."

./scripts/iniciar_brokers.sh
sleep 1

echo "iniciando teste de carga (60s)..."
./bin/carga -duracao 60s -saida resultados/normal > logs/carga_normal.log 2>&1
cat resultados/normal/resumo.txt

./scripts/parar_brokers.sh
