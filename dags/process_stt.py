from asyncio.base_subprocess import logger
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import io
import pandas as pd

BUCKET_NAME = "em-plus"
DATA_DIR = f"s3://{BUCKET_NAME}"

def load_stt(file_path: str) -> pd.DataFrame:
    """
    Load STT CSV file from S3 and return a DataFrame.
    """
    s3_hook = S3Hook(aws_conn_id="s3_data")
    obj = s3_hook.get_key(key=file_path, bucket_name=BUCKET_NAME)
    df = pd.read_csv(io.BytesIO(obj.get()["Body"].read()))
    print(df)
    return df

with DAG(
    dag_id="process_stt",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="Processing of STT CSV files for billing aggregation",
):

    @task
    def process_stt_files():
        # pull stt files from S3
        stt1 = load_stt("source/2025-11/STT1.csv")
        stt2 = load_stt("source/2025-11/STT2.csv")

        stt1["source"] = "STT1"
        stt2["source"] = "STT2"

        # merge STT1 and STT2
        merged = pd.concat([stt1, stt2], ignore_index=True)

        # skip if there's incomplete data
        merged = merged.dropna(ignore_index=True)

        # assume "number" is unique key, keep the last occurence which is from STT2
        merged = merged.drop_duplicates(subset=["number"], keep="last", ignore_index=True)

        merged["date"] = pd.to_datetime(merged["date"]).dt.date

        billing = merged.groupby(["date", "client_code", "client_type"]).agg(
            number_count=("number", "count"),
            amount_total=("amount", "sum"),
        ).reset_index()

        # calculate total amount by client type
        billing["Debit"] = billing.apply(
            lambda x: x["amount_total"] if x["client_type"] == "C" else 0, axis=1
        )
        billing["Credit"] = billing.apply(
            lambda x: x["amount_total"] if x["client_type"] == "V" else 0, axis=1
        )

        billing = billing[["date", "client_code", "number_count", "Debit", "Credit"]]
        print(billing)

        # upload billing result to S3
        s3_hook = S3Hook(aws_conn_id="s3_data")
        output_path = "result/billing_result.csv"
        csv_buffer = io.StringIO()
        billing.to_csv(csv_buffer, index=False)
        s3_hook.load_string(string_data=csv_buffer.getvalue(), key=output_path, bucket_name=BUCKET_NAME, replace=True)

        print("Billing generated at:", f"s3://{BUCKET_NAME}/{output_path}")

    process_stt_files()
