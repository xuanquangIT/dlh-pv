#!/bin/bash
set -e

# Generate Trino catalog configuration from template
echo "Generating Trino Iceberg catalog configuration..."

envsubst < /etc/trino/catalog/iceberg.properties.template > /etc/trino/catalog/iceberg.properties

echo "Trino catalog configuration generated!"
cat /etc/trino/catalog/iceberg.properties
