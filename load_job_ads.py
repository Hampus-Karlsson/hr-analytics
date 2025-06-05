import datetime
import requests
import json
import dlt
import pandas as pd

##Steg 1 bygg api url
## steg 2 hämta data från apin
## steg 3 skicka in datan i duckdb
## steg 4 verifiera att korekt data hämtas
##steg 5

def create_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="job_ads_pipeline",
        destination= "duckdb",
        dataset_name="staging"
    )
    
    print("Creating pipeline...")
    return pipeline

def make_params_list(occupation_field_dict):
    params_list =[]
    print("Building params list...")
    for field_name, field_code in occupation_field_dict.items():
        print(f"Occupation field {field_name}:")

        params = {
            "occupation-field": field_code,
            "limit":  100,
            "offset":0,
        }

        params_list.append(params)
    return params_list

def _get_ads(url_for_search, params):
    headers = {"accept": "application/json"}
    response =requests.get(url_for_search,headers=headers,params=params)
    response.raise_for_status()
    return json.loads(response.content.decode("utf8"))
        

# pipeline resource for fetching job ads ---
@dlt.resource(write_disposition="merge", primary_key="id")
def jobsearch_resource(params_list):
    
    base_url = "https://jobsearch.api.jobtechdev.se" #base url
    url_for_search = f"{base_url}/search"

    for params in params_list:
        limit =params.get("limit", 100) #100 ads per page
        offset =params.get("offset", 0) #0 = first page

        while True:
            page_params = dict(params, offset=offset)

            data = _get_ads(url_for_search,page_params)

            hits =data.get("hits, []")

            if not hits:
                break
            
            for ad in hits:
                yield ad

            if len(hits) < limit or offset > 1900:
                if offset >1900:
                        print("Reached 2000 hits -some ads might be missing")
                break
            
            print(f"Fetched {len(hits)} ads..")

            offset += limit

def run_pipeline(table_name, occupation_field_dict):
    pipeline = create_pipeline()
    params_list = make_params_list(occupation_field_dict)
    
    #runs pipeline
    print("Running pipeline...")
    pipeline.run(
        jobsearch_resource(params_list=params_list),
        table_name=table_name
    )
    print('pipeline completed')



if __name__ == "__main__":
   
    print("Running pipeline...")

    table_name="raw_job_ads"

    #occupation filed dict-mapping
    occupation_field_dict= {
        "Data-it" :"NYW6_mP6_vwf",
        "Bygg-Anläggning" :"NYW6_mP6_vwf",
        "Hälso- och sjukvård" :"NYW6_mP6_vwf"
    }

    run_pipeline(table_name, occupation_field_dict)








    