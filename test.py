import duckdb

con = duckdb.connect(database="job_ads_pipeline.duckdb")
tables = con.execute("SHOW TABLES").fetchall()
print(tables)
