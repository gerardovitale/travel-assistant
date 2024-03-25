#!/bin/bash

source scripts/common.sh

COMMIT_ID=$(git rev-parse --short HEAD)

logger "INFO" "Tagging images"
docker tag update_fuel_prices gerardovitale/travel-assistant-update-fuel-prices:$COMMIT_ID
docker tag update_fuel_prices gerardovitale/travel-assistant-update-fuel-prices:latest

logger "INFO" "Pushing images"
docker login --username $DOCKER_USERNAME --password $DOCKER_PASSWORD docker.io >/dev/null
docker push gerardovitale/travel-assistant-update-fuel-prices:$COMMIT_ID
docker push gerardovitale/travel-assistant-update-fuel-prices:latest
unset DOCKER_PASSWORD
logger "INFO" "Push completed"

logger "INFO" "Cleaning"
yes | docker container prune
yes | docker image prune
yes | docker volume prune
