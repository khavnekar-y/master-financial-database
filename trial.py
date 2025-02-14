import os
import time
import zipfile
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# Configurations
BASE_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
S3_BUCKET_NAME = "financial-statements-database-bucket "  # Replace with your actual S3 bucket
S3_FOLDER = "sec-zips/"  # Folder inside the S3 bucket (optional)
DOWNLOAD_FOLDER = "downloads"  # Local folder to store ZIPs before upload

AWS_SERVER_PUBLIC_KEY=''
AWS_SECRET_KEY = ''
AWS_REGION = 'us-east-2'

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

# Configure Chrome to automatically download files
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode (no GUI)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": DOWNLOAD_FOLDER,  # Set download directory
    "download.prompt_for_download": False,  # Disable download popups
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

# Initialize Selenium WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

def get_zip_elements():
    """Scrape and return all .zip file elements from the SEC website."""
    driver.get(BASE_URL)

    try:
        # Wait for ZIP file links to appear
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '.zip')]"))
        )
    except Exception:
        print("❌ Timeout waiting for .zip links.")
        return []

    # Get ZIP elements (not just URLs)
    return driver.find_elements(By.XPATH, "//a[contains(@href, '.zip')]")

def download_and_extract(zip_elements):
    """Click each ZIP link in Selenium to trigger a browser download, then extract & upload to S3."""
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)  # Ensure the folder exists

    for elem in zip_elements:
        zip_name = elem.text.strip().replace(" ", "_") + ".zip"
        zip_path = os.path.join(DOWNLOAD_FOLDER, zip_name)

        print(f"⬇️ Downloading: {zip_name}")
        elem.click()  # Click the link to start the download
        time.sleep(5)  # Allow time for download to start

        # Wait for the file to appear in the download folder
        while not os.path.exists(zip_path):
            time.sleep(2)

        print(f"📂 Extracting: {zip_name}")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for file in zip_ref.namelist():
                with zip_ref.open(file) as extracted_file:
                    file_data = extracted_file.read()

                    # Upload extracted file to S3
                    s3_key = os.path.join(S3_FOLDER, file)
                    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=file_data)
                    print(f"✅ Uploaded: {file} → s3://{S3_BUCKET_NAME}/{s3_key}")

        # Remove ZIP file after extraction
        os.remove(zip_path)

def main():
    """Main function to scrape, download, extract, and upload to S3."""
    zip_elements = get_zip_elements()

    if not zip_elements:
        print("❌ No .zip files found.")
        return

    print(f"🔍 Found {len(zip_elements)} .zip files.")

    download_and_extract(zip_elements)

    driver.quit()  # Close Selenium browser

if __name__ == "__main__":
    main()
