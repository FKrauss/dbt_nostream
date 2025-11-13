# Overview

Nostream is a dbt (data build tool) project designed for analyzing Nostr event data. The project transforms raw Nostr event streams into structured analytical models using Google BigQuery as the data warehouse. It follows standard dbt project conventions with staging and marts layers for data transformation.

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Data Warehouse
- **Platform**: Google BigQuery
- **Authentication**: Service account credentials with write permissions required
- **Data Source**: Partitioned Nostr event stream table serving as the primary raw data source

## dbt Project Structure
- **Staging Layer** (`models/staging/`): Contains models that clean and standardize raw Nostr event data
  - `stg_nostr_events_all`: Base staging table for all Nostr events
  - `int_profile_metadata`: Parses kind 0 events to extract profile data (username, description, etc.)
  - `int_follower_counts`: Aggregates follower counts per account
  - `int_profile_frequency`: Calculates frequency of usernames/descriptions across accounts
  - `int_state_contact_list_latest`: Latest contact list state
- **Marts Layer** (`models/marts/`): Houses business logic models for analytics, including:
  - `fct_follows`: Fact table for follow relationships
  - `fct_impersonator_flags`: Identifies potential impersonator accounts based on username/description frequency and follower count heuristics (configurable thresholds)
- **Seeds** (`seeds/`): Sample CSV data files for reference data

## Build Tooling
- **Python Version**: 3.11
- **Package Management**: Uses uv for dependency resolution
- **Pre-commit Hooks**: Configured for code quality enforcement
- **CI/CD**: GitHub Actions workflow for automated testing and deployment
  - Triggers on push/PR to main branch
  - Manual workflow dispatch for staging/prod environments
  - Automated dbt testing on each run

## Development Dependencies
Core Python packages include:
- dbt-core and dbt-bigquery for data transformation
- pre-commit for code quality
- typer and rich for CLI functionality
- jafgen for fake data generation
- Standard testing and utility libraries (pytest, faker, requests)

# External Dependencies

## dbt Packages
- **dbt_utils** (v1.1.1): Core utility macros for cross-database compatibility, SQL generation, and generic tests
- **audit_helper** (v0.6.0): Data auditing macros for comparing query results and validating transformations
- **dbt_date** (v0.10.0): Date manipulation and calendar functionality macros

## Cloud Services
- **Google BigQuery**: Primary data warehouse
  - Requires `GCP_SERVICE_ACCOUNT_CREDENTIALS` environment variable (service account JSON)
  - Configured with staging, production, and test targets
  - All profiles use `keyfile_json` with environment variable for credentials

## Model Features

### Impersonator Detection (`fct_impersonator_flags`)
Identifies potential impersonator accounts using configurable heuristics:

**Configurable Thresholds** (set via `dbt_project.yml` vars):
- `username_frequency_threshold`: Minimum accounts with same username to flag (default: 5)
- `description_frequency_threshold`: Minimum accounts with same description to flag (default: 3)  
- `follower_ratio_threshold`: Maximum follower ratio vs. top account (default: 0.10 = 10%)

**Detection Logic**: Flags accounts that share common usernames/descriptions with many others but have disproportionately low follower counts compared to the most-followed account with that same identifier.

**Output Fields**: 
- Risk score (0-1+, higher = more suspicious)
- Boolean flags for username/description-based detection
- Frequency counts and follower ratios for analysis

## CI/CD Infrastructure
- **GitHub Actions**: Automated testing and deployment pipeline
- **Docker**: Used for local development environment (particularly for Postgres testing of dbt packages)