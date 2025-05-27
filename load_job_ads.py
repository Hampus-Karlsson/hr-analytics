import dlt
import requests 
import duckdb

#pipeline

pipeline =dlt.pipeline(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="raw_data"
)

# Hämta annonser
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

# tre olika yrkestyper
data_engineering = get_job_ads("data-it")
construction = get_job_ads("bygg-anläggning")
healthcare =get_job_ads("hälso-och-sjukvård")

#anropa pipeline
pipeline.run([
    {"occupation_field": "data-it", "ads":data_engineering},
    {"occupation_field": "bygg-anläggning", "ads": construction},
    {"occupation_field": "hälso-och-sjukvård", "ads": healthcare}
])