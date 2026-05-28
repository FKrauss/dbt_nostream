
#!/bin/bash
# Write the bigquery_json secret to a temporary file for dbt to use
echo "$bigquery_json" > /tmp/gcp_credentials.json
chmod 600 /tmp/gcp_credentials.json
