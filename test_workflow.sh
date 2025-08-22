#!/bin/bash

# Local testing script that simulates the GitHub Actions workflow
# Usage: ./test_workflow.sh [stage|prod]

set -e

# Default to stage if no target specified
TARGET="${1:-stage}"
echo "Testing with target: $TARGET"

# Check if GCP_SERVICE_ACCOUNT_CREDENTIALS is set
if [ -z "$GCP_SERVICE_ACCOUNT_CREDENTIALS" ]; then
    echo "Error: GCP_SERVICE_ACCOUNT_CREDENTIALS environment variable is not set"
    echo "Please set it with your service account JSON:"
    echo "export GCP_SERVICE_ACCOUNT_CREDENTIALS='{\"type\": \"service_account\", ...}'"
    exit 1
fi

# Create the same profiles.yml that GitHub Actions creates
echo "Creating profiles.yml..."
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
dbt_nostream:
  target: stage
  outputs:
    stage:
      type: bigquery
      method: service-account
      keyfile_json: "{{ env_var('GCP_SERVICE_ACCOUNT_CREDENTIALS') }}"
      project: replit-gcp
      dataset: dbt_nostream_stage
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
      retries: 1
    prod:
      type: bigquery
      method: service-account
      keyfile_json: "{{ env_var('GCP_SERVICE_ACCOUNT_CREDENTIALS') }}"
      project: replit-gcp
      dataset: dbt_nostream_prod
      threads: 8
      timeout_seconds: 300
      location: US
      priority: interactive
      retries: 1
EOF

# Debug: Show profiles.yml content
echo "Profiles.yml content:"
cat ~/.dbt/profiles.yml

# Debug: Check environment variable
echo "GCP_SERVICE_ACCOUNT_CREDENTIALS is set: $([ -n "$GCP_SERVICE_ACCOUNT_CREDENTIALS" ] && echo "YES" || echo "NO")"

# Install dependencies (if needed)
echo "Installing dependencies..."
pip install dbt-core dbt-bigquery

# Install dbt packages
echo "Installing dbt packages..."
dbt deps

# Run dbt models
echo "Running dbt models with target: $TARGET"
dbt run --target $TARGET
dbt test --target $TARGET

echo "✅ Local testing completed successfully!" 