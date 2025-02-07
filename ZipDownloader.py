import os
import time
import requests
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
S3_BUCKET_NAME = "financial-statements-database-bucket "  # Replace with your actual S3 bucket
S3_FOLDER = "sec-zips/"  # Folder inside the S3 bucket (optional)
DOWNLOAD_FOLDER = "downloads"  # Local folder to store ZIPs before upload

AWS_SERVER_PUBLIC_KEY=''
AWS_SECRET_KEY = ''
AWS_REGION = ''  


# AWS Configuration
#AWS_SERVER_PUBLIC_KEY = os.getenv("AWS_SERVER_PUBLIC_KEY")
#AWS_SECRET_KEY = os.getenv("AWS_SERVER_SECRET_KEY")
#AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
#AWS_REGION = os.getenv("AWS_REGION")  # e.g., 'us-east-1'

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

def get_zip_links():
    """Scrape and return all .zip file elements from the SEC website."""
    driver.get(BASE_URL)

    # Scroll down to ensure JavaScript loads all elements
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Allow time for new content to load
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    try:
        # Wait for ZIP file links to appear
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '.zip')]"))
        )
    except Exception:
        print("❌ Timeout waiting for .zip links.")
        return []

    # Extract .zip elements (not just URLs)
    zip_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '.zip')]")

    return zip_elements

def download_zips(zip_elements):
    """Click each ZIP link in Selenium to trigger a browser download."""
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)  # Ensure download folder exists

    for elem in zip_elements:
        file_name = elem.text.strip().replace(" ", "_") + ".zip"
        print(f"⬇️ Downloading: {file_name}")
        elem.click()  # Click the link to start the download
        time.sleep(2)  # Allow time for download to start

    # Wait for downloads to complete
    print("⌛ Waiting for downloads to finish...")
    time.sleep(15)  # Adjust this based on your internet speed

def upload_to_s3():
    """Upload downloaded ZIPs to S3."""
    for file_name in os.listdir(DOWNLOAD_FOLDER):
        file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
        s3_key = os.path.join(S3_FOLDER, file_name).replace("\\", "/")

        try:
            s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_key)
            print(f"✅ Uploaded {file_name} to s3://{S3_BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"❌ Failed to upload {file_name} to S3: {e}")

def main():
    """Main function to scrape, download, and upload .zip files."""
    zip_elements = get_zip_links()

    if not zip_elements:
        print("❌ No .zip files found.")
        return

    print(f"🔍 Found {len(zip_elements)} .zip files.")

    download_zips(zip_elements)
    upload_to_s3()

    driver.quit()  # Close Selenium browser

if __name__ == "__main__":
    main()
