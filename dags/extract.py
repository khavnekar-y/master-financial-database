import os
import time
import zipfile
import random
import boto3
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta

# =========================
# SELENIUM IMPORTS
# =========================
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# =========================
# DAG DEFAULT ARGS
# =========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),  # Adjust as needed
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# =========================
# CONSTANTS / CONFIG
# =========================
BASE_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"

S3_BUCKET_NAME = "aimeet"
S3_FOLDER = "sec-zips/"
DOWNLOAD_FOLDER = os.path.abspath("downloads")
EXTRACTED_FOLDER = os.path.abspath("extracted")

AWS_SERVER_PUBLIC_KEY = "AKIA47CRWJS4E3W5S2GB"
AWS_SECRET_KEY = "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"
AWS_REGION = "us-east-2"

# Rotating User-Agents to reduce 403 risk
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
]

# =========================
# HELPER FUNCTIONS
# =========================

def wait_for_downloads(download_folder, timeout=60):
    """
    Wait until downloads are complete (no *.crdownload files) or until timeout (seconds).
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if any(f.endswith(".crdownload") for f in os.listdir(download_folder)):
            time.sleep(2)  # still downloading
        else:
            print("✅ All downloads completed.")
            return True
    print("❌ Timeout: downloads did not complete.")
    return False

def upload_extracted_to_s3(s3_client, extracted_root):
    """
    Upload extracted files to S3, then remove local files.
    """
    for root, _, files in os.walk(extracted_root):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            s3_key = os.path.join(S3_FOLDER, os.path.relpath(file_path, extracted_root)).replace("\\", "/")

            for attempt in range(3):
                try:
                    s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_key)
                    print(f"✅ Uploaded {file_name} to s3://{S3_BUCKET_NAME}/{s3_key}")
                    break
                except Exception as e:
                    print(f"⚠️ Attempt {attempt + 1}: Failed to upload {file_name} to S3: {e}")
                    time.sleep(2)

            # Optionally remove local extracted file after upload
            os.remove(file_path)
            print(f"🗑️ Deleted local extracted file: {file_name}")

# =========================
# MAIN AIRFLOW TASK
# =========================
def main_task(**context):
    """
    Single main task that:
    1) Reads year/quarter from DAG run config or defaults
    2) Uses Selenium to find the needed ZIP link on SEC
    3) Clicks link to download
    4) Extracts files
    5) Uploads to S3
    """
    # You can read run_id or dag_run.conf for dynamic year/quarter
    dag_run = context["dag_run"]
    conf = getattr(dag_run, "conf", {}) or {}
    year = conf.get("year", "2024")
    quarter = conf.get("quarter", "1")

    print(f"📅 Target: Year={year} Quarter={quarter}")
    required_zip = f"{year}q{quarter}.zip"

    # =======================
    # 1) Setup S3 Client
    # =======================
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    # =======================
    # 2) Configure Selenium
    # =======================
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    os.makedirs(EXTRACTED_FOLDER, exist_ok=True)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Setup automatic downloads
    prefs = {
        "download.default_directory": DOWNLOAD_FOLDER,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    # Add random user-agent
    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # =======================
    # 3) Scrape .zip link
    # =======================
    driver.get(BASE_URL)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '.zip')]"))
        )
        zip_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.zip')]")
        if not zip_links:
            raise AirflowFailException("❌ No .zip links found on the page.")
    except Exception as e:
        driver.quit()
        raise AirflowFailException(f"❌ Error during scraping ZIP links: {str(e)}")

    # Filter only the needed quarter's .zip
    matching_links = [elem for elem in zip_links if required_zip in elem.get_attribute("href")]
    if not matching_links:
        driver.quit()
        raise AirflowFailException(f"❌ No ZIP file found for {year}-Q{quarter}.")

    print(f"✅ Found {len(matching_links)} matching link(s) for {required_zip}.")

    # =======================
    # 4) Download the .zip
    # =======================
    for link_elem in matching_links:
        link_url = link_elem.get_attribute("href")
        print(f"⬇️ Starting download for: {link_url}")
        link_elem.click()
        time.sleep(2)  # Let the download begin

    # Wait for all downloads to finish
    download_success = wait_for_downloads(DOWNLOAD_FOLDER, timeout=60)
    driver.quit()

    if not download_success:
        raise AirflowFailException("❌ Downloads did not complete in time.")

    # =======================
    # 5) Extract All Zips
    # =======================
    for file_name in os.listdir(DOWNLOAD_FOLDER):
        if file_name.endswith(".zip"):
            zip_path = os.path.join(DOWNLOAD_FOLDER, file_name)
            extract_path = os.path.join(EXTRACTED_FOLDER, file_name.replace(".zip", ""))
            os.makedirs(extract_path, exist_ok=True)
            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(extract_path)
                print(f"📂 Extracted: {file_name} => {extract_path}")
            except zipfile.BadZipFile:
                print(f"❌ Corrupt ZIP file: {file_name}")
            # Optionally remove the downloaded ZIP after extraction
            os.remove(zip_path)

    # =======================
    # 6) Upload to S3
    # =======================
    upload_extracted_to_s3(s3_client, EXTRACTED_FOLDER)
    print("🎉 Completed main task.")

# =========================
# 7) DEFINE AIRFLOW DAG
# =========================
dag = DAG(
    "selenium_sec_pipeline",
    default_args=default_args,
    description="Use Selenium to scrape SEC data, download, extract, and upload to S3",
    schedule_interval=None,  # Change to @daily if needed
    catchup=False,
)

# Single operator for entire process
main_operator = PythonOperator(
    task_id="selenium_scrape_download_extract_upload",
    python_callable=main_task,
    dag=dag,
)

