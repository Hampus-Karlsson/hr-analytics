import requests
import dlt

# Hämta yrkesfält (occupation fields)
def get_occupation_fields():
    url = "https://jobsearch.api.jobtechdev.se/metadata/occupationfields"
    headers = {"Accept": "application/json;version=2"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

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

def ads_resource(occupation_field_id):
    ads = get_job_ads(occupation_field_id)
    for ad in ads:
        yield ad
#koddel

if __name__ == "__main__":
    fields = get_occupation_fields()
    # Hitta några yrkesfält id:n (exempel)
    print("Några yrkesfält:")
    for f in fields:
        print(f"{f['id']}, Namn: {f['label']['sv']}")

    # Ta id för t.ex. "Data-IT" eller liknande från utskriften
    data_it_id = None
    bygg_id = None
    halsa_id = None

    for f in fields:
        if "data" in f['label']['sv'].lower():
            data_it_id = f['id']
        elif "bygg" in f['label']['sv'].lower():
            bygg_id = f['id']
        elif "hälso" in f['label']['sv'].lower():
            halsa_id = f['id']

    print(f"Data IT id: {data_it_id}")
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