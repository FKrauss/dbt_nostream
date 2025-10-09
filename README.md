# Nostream

A simple dbt project for nostr event data.

## how to use

the current project requires two things
1. Google Bigquery credentials - with write permissions
2. a nostr event stream table (preferably partitioned). it is the main data source for building the models.

note: more will be shared soon 

## Project Structure

- `models/staging/` - Staging models that clean and standardize raw data
- `models/marts/` - Business logic models for analytics
- `seeds/` - Sample CSV data files

# contributing

thus far, this has been a side project of mine. if you have use cases, pull requests or just ideas, please reach out on [nostr](nostr:npub1qtgmfq7jruka7l7q96gjlk4ar2ljjnsmca6l4l3atwldhyz7q5aqwnh07s)
