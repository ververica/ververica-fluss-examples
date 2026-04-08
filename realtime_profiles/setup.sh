#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_DIR="$SCRIPT_DIR/jars"
ARTIFACT="fluss-flink-realtime-profile-0.1.0.jar"

echo "==> Building project..."
mvn -f "$SCRIPT_DIR/pom.xml" clean package -q

echo "==> Copying jar to $JAR_DIR..."
mkdir -p "$JAR_DIR"
cp "$SCRIPT_DIR/target/$ARTIFACT" "$JAR_DIR/$ARTIFACT"

echo "==> Starting services..."
docker compose -f "$SCRIPT_DIR/docker-compose.yaml" up
