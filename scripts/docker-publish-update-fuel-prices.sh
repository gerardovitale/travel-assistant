#!/bin/bash

source scripts/common.sh

COMMIT_ID=$(git rev-parse --short HEAD)

logger "INFO" "Logging in Docker Hub"
docker login --username "$DOCKERHUB_USERNAME" --password-stdin

logger "INFO" "Tagging and pushing image for $COMMIT_ID"
docker tag "$DOCKER_LOCAL_IMAGE_NAME" gerardovitale/travel-assistant-update-fuel-prices:"$COMMIT_ID"
docker push gerardovitale/travel-assistant-update-fuel-prices:"$COMMIT_ID"

logger "INFO" "Tagging and pushing image for latest"
docker tag "$DOCKER_LOCAL_IMAGE_NAME" gerardovitale/travel-assistant-update-fuel-prices:latest
docker push gerardovitale/travel-assistant-update-fuel-prices:latest
unset DOCKER_PASSWORD
logger "INFO" "Push completed"
