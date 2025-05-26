import dlt
import requests 
import duckdb

#pipeline

pipeline =dlt.pipeline(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="raw_data"
)

