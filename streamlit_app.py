import streamlit as st
import duckdb
import pandas as pd

# Rubrik
st.title("Jobbannonser per yrkesområde")
st.markdown("Data hämtad från Jobtech API via DLT och lagrad i DuckDB")

# Ladda data
@st.cache_data
def load_data():
    con = duckdb.connect(database="job_ads_pipeline.duckdb")
    df = con.execute("SELECT * FROM raw_job_ads").fetchdf()
    return df

df = load_data()

# Visa datan
if df.empty:
    st.warning("Ingen data hittades!")
else:
    st.success(f"{len(df)} annonser laddade!")

    # Kolla vilka fält som finns
    st.subheader("Exempelfält i datan:")
    st.write(df.columns)

    # Räkna antal annonser per yrkesområde (t.ex. via 'occupation.label')
    if "occupation.label" in df.columns:
        st.subheader("Antal annonser per yrke:")
        counts = df["occupation.label"].value_counts().reset_index()
        counts.columns = ["Yrke", "Antal"]
        st.bar_chart(counts.set_index("Yrke"))
    else:
        st.error("Fältet 'occupation.label' hittades inte i datan.")

    # Visa tabell
    st.subheader("Exempelannonser:")
    st.dataframe(df[["headline", "occupation.label", "employer.name"]].dropna().head(10))
