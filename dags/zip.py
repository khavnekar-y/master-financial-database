from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import time
import zipfile
import requests  # ✅ Import fixed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "sec_data_pipeline",
    default_args=default_args,
    description="Scrape SEC financial statement data, upload to S3, and load to Snowflake",
    schedule_interval="@daily",
    catchup=False,
)

# AWS & Snowflake Configs
S3_BUCKET_NAME = "financial-statements-database-bucket"
S3_FOLDER = "sec-data/"
EXTRACTED_FOLDER = "/opt/airflow/tmp/sec_extracted"  # ✅ Fixed path

BASE_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"

# Function to scrape ZIP URLs
def scrape_zip_urls(**context):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    )
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    driver.get(BASE_URL)
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '.zip')]"))
    )
    zip_links = [elem.get_attribute("href") for elem in driver.find_elements(By.XPATH, "//a[contains(@href, '.zip')]")]
    driver.quit()

    context["task_instance"].xcom_push(key="zip_links", value=zip_links)
    return zip_links

# Function to download ZIP files
def download_zips(**context):
    zip_links = context["task_instance"].xcom_pull(task_ids="scrape_zip_urls", key="zip_links")
    download_folder = "/opt/airflow/tmp/sec_zips"  # ✅ Fixed path
    os.makedirs(download_folder, exist_ok=True)

    for link in zip_links:
        file_name = os.path.join(download_folder, os.path.basename(link))
        
        # 🛑 Check if the link is accessible
        response = requests.get(link, stream=True)
        if response.status_code != 200:
            print(f"⚠️ Skipping {link} - HTTP {response.status_code}")
            continue

        # ✅ Save the ZIP file
        with open(file_name, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        time.sleep(2)  # Pause between downloads

        # 🛑 Validate if it's a real ZIP file
        if not zipfile.is_zipfile(file_name):
            print(f"⚠️ Skipping {file_name} - Not a valid ZIP file")
            os.remove(file_name)  # Remove the invalid file

    return download_folder

# Function to extract ZIP files
def extract_zips(**context):
    zip_folder = context["task_instance"].xcom_pull(task_ids="download_zips")
    os.makedirs(EXTRACTED_FOLDER, exist_ok=True)

    for file_name in os.listdir(zip_folder):
        zip_path = os.path.join(zip_folder, file_name)

        # 🛑 Check if it's actually a ZIP file before extracting
        if not zipfile.is_zipfile(zip_path):
            print(f"⚠️ Skipping {zip_path} - Not a valid ZIP file")
            continue

        extract_path = os.path.join(EXTRACTED_FOLDER, file_name.replace(".zip", ""))
        os.makedirs(extract_path, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)

    return EXTRACTED_FOLDER

# Function to upload extracted files to S3
def upload_to_s3(**context):
    extracted_folder = context["task_instance"].xcom_pull(task_ids="extract_zips")
    s3_hook = S3Hook(aws_conn_id="aws_default")  # ✅ Uses secure AWS connection

    for root, _, files in os.walk(extracted_folder):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            s3_key = os.path.join(S3_FOLDER, os.path.relpath(file_path, extracted_folder)).replace("\\", "/")

            s3_hook.load_file(
                filename=file_path,
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True
            )

# Snowflake tasks
create_stage = SnowflakeOperator(
    task_id="create_s3_stage",
    snowflake_conn_id="snowflake_default",
    sql=f"""
    CREATE STAGE IF NOT EXISTS sec_stage
    URL='s3://{S3_BUCKET_NAME}/{S3_FOLDER}'
    CREDENTIALS=(AWS_KEY_ID='{{{{ conn.aws_default.login }}}}'
                 AWS_SECRET_KEY='{{{{ conn.aws_default.password }}}}');
    """,
    dag=dag,
)

create_table = SnowflakeOperator(
    task_id="create_snowflake_table",
    snowflake_conn_id="snowflake_default",
    sql="""
    CREATE TABLE IF NOT EXISTS sec_financials (
        company VARCHAR,
        filing_date DATE,
        total_assets FLOAT,
        total_liabilities FLOAT,
        net_income FLOAT
    );
    """,
    dag=dag,
)

create_file_format = SnowflakeOperator(
    task_id="create_file_format",
    snowflake_conn_id="snowflake_default",
    sql="""
    CREATE OR REPLACE FILE FORMAT csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        NULL_IF = ('NULL', 'null')
        EMPTY_FIELD_AS_NULL = TRUE
        DATE_FORMAT = 'YYYY-MM-DD';
    """,
    dag=dag,
)

load_to_snowflake = SnowflakeOperator(
    task_id="load_to_snowflake",
    snowflake_conn_id="snowflake_default",
    sql="""
    COPY INTO sec_financials
    FROM @sec_stage
    FILE_FORMAT = csv_format
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag,
)

# Define tasks
scrape_task = PythonOperator(
    task_id="scrape_zip_urls",
    python_callable=scrape_zip_urls,
    dag=dag,
)

download_task = PythonOperator(
    task_id="download_zips",
    python_callable=download_zips,
    dag=dag,
)

extract_task = PythonOperator(
    task_id="extract_zips",
    python_callable=extract_zips,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)

# DAG task dependencies
scrape_task >> download_task >> extract_task >> upload_task
upload_task >> create_stage >> create_file_format >> create_table >> load_to_snowflake
