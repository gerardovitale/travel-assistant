#!/bin/bash

source scripts/common.sh

COMMIT_ID=$(git rev-parse --short HEAD)

logger "INFO" "Logging in Docker Hub"
docker login --username "$DOCKER_HUB_USERNAME" --password-stdin

logger "INFO" "Tagging and pushing image for $COMMIT_ID"
docker tag "$DOCKER_LOCAL_IMAGE_NAME" "$DOCKER_HUB_USERNAME"/"$PROJECT_NAME"-"$DOCKER_LOCAL_IMAGE_NAME":"$COMMIT_ID"
docker push "$DOCKER_HUB_USERNAME"/"$PROJECT_NAME"-"$DOCKER_LOCAL_IMAGE_NAME":"$COMMIT_ID"

logger "INFO" "Tagging and pushing image for latest"
docker tag "$DOCKER_LOCAL_IMAGE_NAME" "$DOCKER_HUB_USERNAME"/"$PROJECT_NAME"-"$DOCKER_LOCAL_IMAGE_NAME":latest
docker push "$DOCKER_HUB_USERNAME"/"$PROJECT_NAME"-"$DOCKER_LOCAL_IMAGE_NAME":latest
logger "INFO" "Push completed"
