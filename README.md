# data-pipeline-cms

Phase 1: Extraction

Accomplished:  
- Ingested raw data from live government API using dynamic URL resolution
- Uploaded raw data into S3
- Structured logging, error handling

Phase 2:

Accomplished:
- Read raw data from S3 and write Pandas DataFrames to S3
- Clean both datasets by normalizing, striping white space, replacing nulls, etc.
- Uploaded cleaned data into S3