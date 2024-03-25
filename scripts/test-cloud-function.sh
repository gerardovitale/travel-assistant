#!/bin/bash

source .env

URI=$(gcloud functions describe $FUNCT_NAME --gen2 --region $REGION --format="value(serviceConfig.uri)")

curl -m 70 -X POST $URI \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{}'
