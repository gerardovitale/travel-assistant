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
    --set-env-vars MACHINE_TYPE="$MACHINE_TYPE"
