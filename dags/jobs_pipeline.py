# Standard Python libraries 
from datetime import datetime, timedelta

# Airflow 
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Hook to connect to PostgreSQL and run queries
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Third party libraries 
import requests                          # Makes HTTP web requests
from bs4 import BeautifulSoup            # HTML to extract data
import pandas as pd                      # Data manipulation and cleaning


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def fetch_job_listings(num_jobs: int, ti) -> None:
    
    api_url = "https://remoteok.com/api"

    print(f"[EXTRACT] Requesting job data from: {api_url}")

    try:
        
        response = requests.get(api_url, headers=HEADERS, timeout=30)

        response.raise_for_status()  

    except requests.exceptions.Timeout:
        raise RuntimeError("[EXTRACT] Request timed out after 30 seconds.")
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"[EXTRACT] HTTP error occurred: {e}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"[EXTRACT] Failed to reach the website: {e}")

 
    all_jobs = response.json()
    job_listings = [item for item in all_jobs if isinstance(item, dict) and "position" in item]

    print(f"[EXTRACT] Total jobs available on the page: {len(job_listings)}")

    jobs = []
    seen_ids = set()  

    for job in job_listings:
        job_id = job.get("id", "")

        if job_id in seen_ids:
            continue  
        seen_ids.add(job_id)

        jobs.append({
            "job_id":    str(job_id),
            "title":     job.get("position", "N/A").strip(),
            "company":   job.get("company", "N/A").strip(),
            "location":  job.get("location", "Remote").strip() or "Remote",
            "tags":      ", ".join(job.get("tags", [])),   # List → comma string
            "salary":    job.get("salary", "N/A") or "N/A",
            "url":       "https://remoteok.com" + job.get("url", ""),
            "posted_at": job.get("date", "N/A"),
        })

        if len(jobs) >= num_jobs:
            break  

    print(f"[EXTRACT] Successfully collected {len(jobs)} job listings.")

    if not jobs:
        raise ValueError("[EXTRACT] No job data was collected. The site structure may have changed.")

    ti.xcom_push(key="raw_job_data", value=jobs)
    print("[EXTRACT] Data pushed to XCom successfully.")


def transform_job_data(ti) -> None:

    raw_data = ti.xcom_pull(key="raw_job_data", task_ids="fetch_job_listings_task")

    if not raw_data:
        raise ValueError("[TRANSFORM] No raw data found in XCom. Did the extract task succeed?")

    print(f"[TRANSFORM] Received {len(raw_data)} raw records. Starting cleaning...")

    df = pd.DataFrame(raw_data)

    print(f"[TRANSFORM] Columns available: {list(df.columns)}")
    print(f"[TRANSFORM] Sample before cleaning:\n{df.head(2).to_string()}")

    before = len(df)
    df.drop_duplicates(subset="job_id", inplace=True)
    print(f"[TRANSFORM] Removed {before - len(df)} duplicate rows.")

    text_columns = ["title", "company", "location", "tags", "salary", "url"]
    for col in text_columns:
        df[col] = df[col].astype(str).str.strip()

    df.replace({"": "N/A", "nan": "N/A", "None": "N/A"}, inplace=True)

    df["url"]  = df["url"].str[:500]   # URLs can get very long
    df["tags"] = df["tags"].str[:300]  # Tags list can also be long

    df["location"] = df["location"].apply(
        lambda x: "Remote" if x.strip() in ("", "N/A", "nan") else x
    )

    print(f"[TRANSFORM] Cleaning complete. {len(df)} clean records ready.")
    print(f"[TRANSFORM] Sample after cleaning:\n{df.head(2).to_string()}")

    clean_data = df.to_dict("records")
    ti.xcom_push(key="clean_job_data", value=clean_data)
    print("[TRANSFORM] Clean data pushed to XCom.")



def insert_jobs_into_postgres(ti) -> None:
   
    clean_data = ti.xcom_pull(key="clean_job_data", task_ids="transform_job_data_task")

    if not clean_data:
        raise ValueError("[LOAD] No clean data found in XCom. Did the transform task succeed?")

    print(f"[LOAD] Preparing to insert {len(clean_data)} records into PostgreSQL...")

    postgres_hook = PostgresHook(postgres_conn_id="jobs_connection")

    insert_query = """
        INSERT INTO job_listings (job_id, title, company, location, tags, salary, url, posted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_id) DO NOTHING;
    """

    inserted_count = 0
    skipped_count = 0

    for job in clean_data:
        try:
            postgres_hook.run(
                insert_query,
                parameters=(
                    job["job_id"],
                    job["title"],
                    job["company"],
                    job["location"],
                    job["tags"],
                    job["salary"],
                    job["url"],
                    job["posted_at"],
                )
            )
            inserted_count += 1
        except Exception as e:
            
            print(f"[LOAD] Warning: Could not insert job_id={job.get('job_id')}: {e}")
            skipped_count += 1

    print(f"[LOAD] Done. Inserted: {inserted_count}, Skipped/Errors: {skipped_count}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),   
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="remote_jobs_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline: scrape remote job listings and store in PostgreSQL",
    schedule_interval=timedelta(days=1),   # Run once every day
    catchup=False,                         # Don't backfill old runs
    tags=["etl", "jobs", "scraping"],      # Tags help organise DAGs in the UI
)


fetch_jobs_task = PythonOperator(
    task_id="fetch_job_listings_task",
    python_callable=fetch_job_listings,
    op_args=[100],   # Fetches up to 100 jobs
    dag=dag,
)

transform_jobs_task = PythonOperator(
    task_id="transform_job_data_task",
    python_callable=transform_job_data,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id="create_table_task",
    postgres_conn_id="jobs_connection",   # Must match the name in Airflow UI
    sql="""
        CREATE TABLE IF NOT EXISTS job_listings (
            id          SERIAL PRIMARY KEY,
            job_id      TEXT UNIQUE NOT NULL,
            title       TEXT,
            company     TEXT,
            location    TEXT,
            tags        TEXT,
            salary      TEXT,
            url         TEXT,
            posted_at   TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag,
)

insert_jobs_task = PythonOperator(
    task_id="insert_jobs_task",
    python_callable=insert_jobs_into_postgres,
    dag=dag,
)


fetch_jobs_task >> transform_jobs_task >> create_table_task >> insert_jobs_task
