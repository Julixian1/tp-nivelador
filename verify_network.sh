#!/bin/bash

NETWORK_NAME=$(docker network ls --filter "name=tp_nivelador" --format "{{.Name}}" | head -n 1)

if [ -z "$NETWORK_NAME" ]; then
  echo "Error: No se encontró la red de Docker. Ejecuta 'make up' primero."
  exit 1
fi

echo "Conectando al servidor en la red '$NETWORK_NAME'..."

echo "Hello World" | docker run --rm -i --network "$NETWORK_NAME" alpine nc server 5678