import duckdb
import pandas as pd
import os
import time
from dotenv import load_dotenv

load_dotenv()  # .env 파일에서 AWS 키 등 불러오기

# ------------------ 환경 설정 ------------------ #
AWS_KEY = os.getenv("AWS_KEY")
AWS_SECRET = os.getenv("AWS_SECRET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = 's3://ldbc-2'
CSV_PATH = f'{S3_BUCKET}/knows.csv'
PARQUET_PATH = f'{S3_BUCKET}/knows.parquet'

# ------------------ DuckDB 초기화 ------------------ #
def setup_duckdb_connection():
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_access_key_id='{AWS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET}';")
    con.execute(f"SET s3_region='{AWS_REGION}';")
    return con

# ------------------ DuckDB Query ------------------ #
def duckdb_group_by(con, path, column):
    start = time.time()
    df = con.execute(f"""
        SELECT "{column}", COUNT(*) AS edge_count
        FROM '{path}'
        GROUP BY "{column}"
        HAVING COUNT(*) >= 200
    """).fetchdf()
    end = time.time()
    print(f"[DuckDB + {os.path.basename(path)}] Time: {end - start:.4f}s")
    return df

# ------------------ Pandas Query ------------------ #
def pandas_group_by(path, column):
    start = time.time()
    if path.endswith('.csv'):
        df = pd.read_csv(path, storage_options={"key": AWS_KEY, "secret": AWS_SECRET})
    else:
        df = pd.read_parquet(path, storage_options={"key": AWS_KEY, "secret": AWS_SECRET})
    df_grouped = df.groupby(column).size().reset_index(name='edge_count')
    df_filtered = df_grouped[df_grouped['edge_count'] >= 200]
    end = time.time()
    print(f"[Pandas + {os.path.basename(path)}] Time: {end - start:.4f}s")
    return df_filtered

# ------------------ Outdegree Query ------------------ #
def duckdb_outdegree(con, path, column=":START_ID"):
    start = time.time()
    df = con.execute(f"""
        SELECT "{column}", COUNT(*) AS outdeg
        FROM '{path}'
        GROUP BY "{column}"
        ORDER BY outdeg DESC
        LIMIT 50
    """).fetchdf()
    end = time.time()
    print(f"[DuckDB Outdegree] Time: {end - start:.4f}s")
    return df

def pandas_outdegree(path, column=":START_ID"):
    start = time.time()
    df = pd.read_parquet(path, storage_options={"key": AWS_KEY, "secret": AWS_SECRET})
    df_out = df.groupby(column).size().reset_index(name='outdeg')
    df_sorted = df_out.sort_values(by='outdeg', ascending=False).head(50)
    end = time.time()
    print(f"[Pandas Outdegree] Time: {end - start:.4f}s")
    return df_sorted

# ------------------ 메타데이터 조회 ------------------ #
def show_parquet_metadata(con, path):
    return con.execute(f"SELECT * FROM parquet_metadata('{path}');").fetchdf()

# ------------------ Main ------------------ #
def main():
    con = setup_duckdb_connection()

    print("\n📊 DuckDB GroupBy CSV")
    print(duckdb_group_by(con, CSV_PATH, "Person1Id").head())

    print("\n📊 DuckDB GroupBy Parquet")
    print(duckdb_group_by(con, PARQUET_PATH, ":START_ID").head())

    print("\n📊 Pandas GroupBy CSV")
    print(pandas_group_by(CSV_PATH, "Person1Id").head())

    print("\n📊 Pandas GroupBy Parquet")
    print(pandas_group_by(PARQUET_PATH, ":START_ID").head())

    print("\n📈 DuckDB Outdegree Top 50")
    print(duckdb_outdegree(con, PARQUET_PATH).head())

    print("\n📈 Pandas Outdegree Top 50")
    print(pandas_outdegree(PARQUET_PATH).head())

    print("\n🧾 Parquet Metadata")
    print(show_parquet_metadata(con, "import/knows.parquet"))

if __name__ == "__main__":
    main()

