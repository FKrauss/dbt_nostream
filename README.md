# Jaffle Shop

A simple dbt project for the Jaffle Shop restaurant data.

## Getting Started

1. Load sample data:
```bash
dbt seed --full-refresh --vars '{"load_source_data": true}'
```

2. Build the project:
```bash
dbt build
```

## Project Structure

- `models/staging/` - Staging models that clean and standardize raw data
- `models/marts/` - Business logic models for analytics
- `seeds/` - Sample CSV data files