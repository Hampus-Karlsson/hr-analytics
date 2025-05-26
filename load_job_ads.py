import dlt
import requests 
import duckdb

#pipeline

pipeline =dlt.pipeline(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="raw_data"
)

# Gather ads
def get_job_ads(occupation_field: str, pages: int = 3):
    all_results = []
    for page in range(1, pages + 1):
        response = requests.get(
            "https://jobsearch.api.jobtechdev.se/search",
            params={"occupation-field": occupation_field, "page": page}
        )
        data = response.json()
        all_results.exted(data.get("hits",[]))
    return all_results