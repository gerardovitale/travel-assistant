#!/bin/bash

source .env || exit 1

gcloud functions deploy "$FUNCT_NAME" \
    --gen2 \
    --service-account="$SERVICE_ACCOUNT" \
    --region="$REGION" \
    --runtime="$RUNTIME" \
    --source="$SOURCE" \
    --entry-point="$ENTRYPOINT" \
    --trigger-http \
    --set-env-vars PROJECT_ID="$PROJECT_ID" \
    --set-env-vars ZONE="$ZONE" \
    --set-env-vars REGION="$REGION" \
    --set-env-vars INSTANCE_NAME="$INSTANCE_NAME" \
    --set-env-vars MACHINE_TYPE="$MACHINE_TYPE" \
    --set-env-vars DOCKER_IMAGE_TO_DEPLOY="$DOCKER_IMAGE_TO_DEPLOY" \
    --set-env-vars PROD="$PROD" \
    --set-env-vars GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS" \
    --set-env-vars DATA_SOURCE_URL="$DATA_SOURCE_URL" \
    --set-env-vars DATA_DESTINATION_BUCKET="$DATA_DESTINATION_BUCKET"
