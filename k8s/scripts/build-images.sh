#!/bin/bash

set -e

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
echo "Building docker images for demo microservices..."

services=(order-service payment-service shipping-service email-service sms-service logging-service analytics-service)

for s in "${services[@]}"; do
  echo "--- Building $s ---"
  docker build -t ${s}:latest "$ROOT_DIR/../$s"
done

echo "All images built."
