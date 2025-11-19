#!/bin/bash

set -e

echo "--- Creating Namespace ---"
kubectl apply -f ../services/namespace.yml

echo "--- Deploying Zookeeper ---"
kubectl apply -f ../services/zookeeper.yml
kubectl wait --for=condition=ready pod -l app=zookeeper -n microservices --timeout=120s

echo "--- Deploying Kafka ---"
kubectl apply -f ../services/kafka.yml
kubectl wait --for=condition=ready pod -l app=kafka -n microservices --timeout=180s

echo "--- Deploying User Service ---"
kubectl apply -f ../services/user-service.yml

echo "--- Waiting for User Service ---"
# FIX: Do not use 'pod --all'. Target the specific app label instead.
kubectl wait --for=condition=ready pod -l app=user-service -n microservices --timeout=180s

echo "--- Deploying Product & Inventory (if present) ---"
kubectl apply -f ../services/product-service.yml || true
kubectl apply -f ../services/inventory-service.yml || true

echo "--- Deploying new microservices ---"
kubectl apply -f ../services/order-service.yml
kubectl apply -f ../services/payment-service.yml
kubectl apply -f ../services/shipping-service.yml
kubectl apply -f ../services/email-service.yml
kubectl apply -f ../services/sms-service.yml
kubectl apply -f ../services/logging-service.yml
kubectl apply -f ../services/analytics-service.yml

echo "--- Waiting for Services to become ready ---"
kubectl wait --for=condition=ready pod -l app=product-service -n microservices --timeout=180s || true
kubectl wait --for=condition=ready pod -l app=inventory-service -n microservices --timeout=180s || true
kubectl wait --for=condition=ready pod -l app=order-service -n microservices --timeout=180s || true
kubectl wait --for=condition=ready pod -l app=payment-service -n microservices --timeout=180s || true
kubectl wait --for=condition=ready pod -l app=shipping-service -n microservices --timeout=180s || true
kubectl wait --for=condition=ready pod -l app=email-service -n microservices --timeout=180s || true
kubectl wait --for=condition=ready pod -l app=sms-service -n microservices --timeout=180s || true
kubectl wait --for=condition=ready pod -l app=logging-service -n microservices --timeout=180s || true
kubectl wait --for=condition=ready pod -l app=analytics-service -n microservices --timeout=180s || true

echo "--- All Resources ---"
kubectl get all -n microservices

echo "--- Port Forwarding ---"

# Keep port-forwards in background so the script can finish. Use & at the end of each.
kubectl port-forward -n microservices svc/user-service 3001:3001 &
kubectl port-forward -n microservices svc/product-service 3002:3002 || true &
kubectl port-forward -n microservices svc/inventory-service 3003:3003 || true &
kubectl port-forward -n microservices svc/order-service 3004:3004 &
kubectl port-forward -n microservices svc/payment-service 3005:3005 &
kubectl port-forward -n microservices svc/shipping-service 3006:3006 &
kubectl port-forward -n microservices svc/email-service 3007:3007 &
kubectl port-forward -n microservices svc/sms-service 3008:3008 &
kubectl port-forward -n microservices svc/logging-service 3009:3009 &
kubectl port-forward -n microservices svc/analytics-service 3010:3010 &

echo "Deployment complete. Services accessible at localhost:3001..3010 (where present)"