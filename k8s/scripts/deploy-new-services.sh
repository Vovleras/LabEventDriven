#!/bin/bash

set -e

echo "Applying new service manifests to namespace 'microservices'"
kubectl apply -f ../services/order-service.yml
kubectl apply -f ../services/payment-service.yml
kubectl apply -f ../services/shipping-service.yml
kubectl apply -f ../services/email-service.yml
kubectl apply -f ../services/sms-service.yml
kubectl apply -f ../services/logging-service.yml
kubectl apply -f ../services/analytics-service.yml

echo "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app=order-service -n microservices --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=payment-service -n microservices --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=shipping-service -n microservices --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=email-service -n microservices --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=sms-service -n microservices --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=logging-service -n microservices --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=analytics-service -n microservices --timeout=120s || true

echo "Deployment complete. You can port-forward services for local access if needed."
