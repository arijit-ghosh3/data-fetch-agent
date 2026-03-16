import pandas as pd
from db.connection import get_connection

QUERY = """
    SELECT js.*
    FROM gdc.Jobstep js
    WHERE js.jobid IN (
        SELECT TOP 1 jobid
        FROM gdc.Job
        WHERE jobguid = ?
        ORDER BY 1 DESC
    )
"""


def read_job_guids(csv_path):
    df = pd.read_csv(csv_path)
    return df["job_guid"].tolist()


def fetch_jobsteps_for_guid(conn, job_guid):
    return pd.read_sql(QUERY, conn, params=[str(job_guid)])


def fetch_all_jobsteps(csv_path):
    guids = read_job_guids(csv_path)
    conn = get_connection()
    results = []

    for guid in guids:
        print(f"Fetching data for Job GUID: {guid}")
        df = fetch_jobsteps_for_guid(conn, guid)
        df["job_guid"] = guid
        results.append(df)

    conn.close()

    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame()
