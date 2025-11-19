#!/bin/bash

set -e  # stop on first error

echo "Starting Minikube..."
# minikube start

echo "Setting Docker environment for Minikube..."
# eval $(minikube -p minikube docker-env)

# Build images for all demo microservices so Kubernetes can pull them from Minikube's Docker daemon
if ! command -v docker >/dev/null 2>&1; then
	echo "Docker CLI not found. Ensure Docker is installed and available." >&2
	exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

services=(
	user-service
	product-service
	inventory-service
	order-service
	payment-service
	shipping-service
	email-service
	sms-service
	logging-service
	analytics-service
)

echo "Building docker images in Minikube's Docker daemon..."
for s in "${services[@]}"; do
	if [ -d "$REPO_ROOT/$s" ]; then
		echo "--- Building $s ---"
		docker build -t ${s}:latest "$REPO_ROOT/$s"
	else
		echo "--- Skipping $s (directory not found) ---"
	fi
done

# Verify images are in Minikube (grep may exit non-zero if none found)
docker images | grep service || true
