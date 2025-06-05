import duckdb

# Anslut till DuckDB-databasen som dlt skapat
con = duckdb.connect("dlt.duckdb")  

# Hämta alla tabeller
tables = con.execute("SHOW TABLES").fetchall()
print("Tables in database:")
for table in tables:
    print("-", table[0])

# Visa antal rader i varje tabell
print("\nRow counts:")
for table in tables:
    table_name = table[0]
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"{table_name}: {count} rows")

# Visa exempelrader (valfritt)
print("\nSample rows:")
for table in tables:
    table_name = table[0]
    df = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
    print(f"\n{table_name} (first 3 rows):")
    print(df)

