import os
import time
import requests
import zipfile
import boto3
from tqdm import tqdm
from urllib.parse import urljoin, urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# Configurations
BASE_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
S3_BUCKET_NAME = "financial-statements-database-bucket"  # ✅ Removed extra space
S3_FOLDER = "sec-zips/"  # Folder inside the S3 bucket
DOWNLOAD_FOLDER = os.path.abspath("downloads")  # Ensure absolute path for downloads
EXTRACTED_FOLDER = os.path.abspath("extracted")  # Folder for extracted contents
selenium_host = "http://localhost:4444"

AWS_SERVER_PUBLIC_KEY = "AKIA54WIGFLOETS7RT4W"
AWS_SECRET_KEY = "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"
AWS_REGION = "us-east-2"  

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

# Configure Chrome to automatically download files
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode (no GUI)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": DOWNLOAD_FOLDER,  # Set download directory
        "download.prompt_for_download": False,  # Disable download popups
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    },
)
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
)

# Initialize Selenium WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Use local ChromeDriver instead of Remote Selenium Grid
def get_zip_elements(year, quarter):
    """Scrape and return only the ZIP file elements for the specified year and quarter."""
    driver.get(BASE_URL)

    try:
        # Wait for ZIP file links to appear
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '.zip')]"))
        )
        
        # Format the expected ZIP filename
        required_zip = f"{year}q{quarter}.zip"

        # Filter and return only matching ZIP elements
        zip_elements = [
            elem for elem in driver.find_elements(By.XPATH, "//a[contains(@href, '.zip')]")
            if required_zip in elem.get_attribute("href")  # Ensure we only return the required ZIP file
        ]

        if zip_elements:
            print(f"✅ Found {len(zip_elements)} matching ZIP file(s) for {year}-Q{quarter}.")
        else:
            print(f"❌ No ZIP file found for {year}-Q{quarter}.")

        return zip_elements

    except Exception as e:
        print(f"❌ Error during ZIP scraping: {str(e)}")
        return []




def extract_zips():
    """Extract downloaded ZIP files to a folder."""
    os.makedirs(EXTRACTED_FOLDER, exist_ok=True)

    for file_name in os.listdir(DOWNLOAD_FOLDER):
        if file_name.endswith(".zip"):
            zip_path = os.path.join(DOWNLOAD_FOLDER, file_name)
            extract_path = os.path.join(EXTRACTED_FOLDER, file_name.replace(".zip", ""))

            os.makedirs(extract_path, exist_ok=True)

            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(extract_path)
                print(f"📂 Extracted: {file_name}")
            except zipfile.BadZipFile:
                print(f"❌ Corrupt ZIP file: {file_name}")

def wait_for_downloads():
    """Wait until all downloads in the folder are complete."""
    print("⌛ Waiting for downloads to complete...")

    timeout = 60  # Maximum wait time in seconds
    elapsed_time = 0

    while elapsed_time < timeout:
        time.sleep(5)
        elapsed_time += 5

        # Check if any ".crdownload" files exist (indicating unfinished downloads)
        if any(file.endswith(".crdownload") for file in os.listdir(DOWNLOAD_FOLDER)):
            continue  # Keep waiting

        print("✅ All downloads completed.")
        return True

    print("❌ Timeout: Downloads did not complete within the expected time.")
    return False

def download_zips(zip_elements):
    """Click each ZIP link in Selenium to trigger a browser download."""
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)  # Ensure download folder exists

    for elem in zip_elements:
        file_name = elem.text.strip().replace(" ", "_") + ".zip"
        print(f"⬇️ Downloading: {file_name}")
        elem.click()  # Click the link to start the download
        time.sleep(2)  # Allow time for download to start

    # ✅ Properly wait for downloads to finish
    wait_for_downloads()



def upload_extracted_to_s3():
    """Upload extracted files to S3."""
    if not os.path.exists(EXTRACTED_FOLDER):
        print("❌ Extracted folder not found.")
        return

    for root, _, files in os.walk(EXTRACTED_FOLDER):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            s3_key = os.path.join(S3_FOLDER, os.path.relpath(file_path, EXTRACTED_FOLDER)).replace("\\", "/")

            for attempt in range(3):
                try:
                    s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_key)
                    print(f"✅ Uploaded {file_name} to s3://{S3_BUCKET_NAME}/{s3_key}")
                    break
                except Exception as e:
                    print(f"⚠️ Attempt {attempt + 1}: Failed to upload {file_name} to S3: {e}")
                    time.sleep(2)

            os.remove(file_path)
            print(f"🗑️ Deleted local extracted file: {file_name}")

def main():
    """Main function to scrape, download, and upload .zip files."""
    year = input("Enter year (e.g., 2024): ")
    quarter = input("Enter quarter (1-4): ")
    
    
    zip_elements = get_zip_elements(year, quarter)

    

    if zip_elements:
        print(f"✅ Found {len(zip_elements)} ZIP file(s) for {year}-Q{quarter}.")
    else:
        print(f"⚠️ No ZIP files found for {year}-Q{quarter}.")

    download_zips(zip_elements)
    extract_zips()
    upload_extracted_to_s3()

    driver.quit()  # Close Selenium browser

if __name__ == "__main__":
    main()
