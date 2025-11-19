#!/bin/bash

# Delete by name and tag
docker rmi order-service:latest
docker rmi inventory-service:latest
docker rmi notification-service:latest