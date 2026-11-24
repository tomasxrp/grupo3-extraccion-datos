#!/bin/bash
TOTAL=3000000

for C in 1 2 4 6; do
  MSG=$((TOTAL / C))
  echo "=========================================="
  echo "PROBANDO CON $C CONSUMIDOR(ES) EN PARALELO ($MSG msg c/u)..."
  echo "=========================================="
  
  START=$(date +%s%3N)
  for ((i=1; i<=C; i++)); do
    ./kafka-consumer-perf-test.sh \
      --bootstrap-server localhost:9092 \
      --topic exp3-concurrencia \
      --num-records $MSG \
      --group "grupo-v2-${C}c" \
      --hide-header &
  done
  wait
  END=$(date +%s%3N)
  ELAPSED=$((END - START))
  echo "--> Tiempo total real de ejecución ($C consumidores): ${ELAPSED} ms"
  echo ""
  sleep 2
done