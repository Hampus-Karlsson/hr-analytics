from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import requests
import dlt


# Hämta yrkesfält (occupation fields) från graphQL
def get_occupation_fields_from_taxonomy():
    transport = RequestsHTTPTransport(
    url = "https://taxonomy.api.jobtechdev.se/v1/graphql",
    headers = {"Accept": "application/json"}
    verify=True,
    retries=3,
    )

    client =Client(transport=transport, fetch_schema_from_transport=True)

    query = gql("""
        query {
            concepts(type:"occupation-field"){
                id
                preferred_label
                }
            }
    """)

    result = client.execute(query)
    return result["concepts"]

##anonshämtning
def get_job_ads(occupation_field_id, page=0, size=100):
    url = "https://jobsearch.api.jobtechdev.se/search"
    headers = {"Accept": "application/json;version=2"}
    params = {
        "occupation-field": occupation_field_id,
        "page": page,
        "size": size
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("results", [])

#dlt
def ads_resource(ads):
    for ad in ads:
        yield ad

#Main
if __name__ == "__main__":
    fields = get_occupation_fields_from_taxonomy()
    
    print("Några yrkesfält:")
   
   data_it_id =next((f['id'] for f in fields if "data" in f['preferred_label']))

    print(f"Data/IT id: {data_it_id}")
    print(f"Bygg id: {bygg_id}")
    print(f"Hälsa id: {halsa_id}")

    # Hämta annonser
    data_engineering = list(get_job_ads(data_it_id))
    construction = list(get_job_ads(bygg_id))
    healthcare = list(get_job_ads(halsa_id))

    print(f"Antal data-it-annonser: {len(data_engineering)}")
    print(f"Antal bygg-annonser: {len(construction)}")
    print(f"Antal hälsoannonser: {len(healthcare)}")

    # DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="job_ads_pipeline",
        destination="duckdb",
        dataset_name="raw_data"
    )

    load_info = pipeline.run([
        dlt.resource(data_engineering, name="data_it_ads"),
        dlt.resource(construction, name="bygg_ads"),
        dlt.resource(healthcare, name="halsa_ads")
    ])

    print(load_info)