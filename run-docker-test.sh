#!/bin/bash

source scripts/logger.sh

# Check if subdirectory name is provided
if [ -z "$1" ]; then
  logger "ERROR" "❌ You must provide the subdirectory name (e.g., 'ingestor')"
  logger "ERROR" "👉 Usage: ./run_test_docker.sh <subdir>"
  exit 1
fi

SUBDIR="$1"
IMAGE_NAME="${SUBDIR}-test"

# Check if the directory exists
if [ ! -d "$SUBDIR" ]; then
  logger "ERROR" "❌ Directory '$SUBDIR' does not exist"
  exit 1
fi

# Check if the Dockerfile.test exists
if [ ! -f "$SUBDIR/Dockerfile.test" ]; then
  logger "ERROR" "❌ Dockerfile.test not found in '$SUBDIR'"
  exit 1
fi

# Build and run Docker test image
logger "INFO" "🐳 Building Docker image for $SUBDIR"
cd "$SUBDIR" && docker buildx build -f Dockerfile.test -t "$IMAGE_NAME" . && cd - || exit
logger "INFO" "🚀 Running Docker container for $IMAGE_NAME"
docker run --rm "$IMAGE_NAME":latest
