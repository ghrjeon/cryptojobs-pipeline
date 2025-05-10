from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import time
import logging
from typing import Dict, List
import os
import subprocess
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import argparse

load_dotenv()

url= os.getenv('SUPABASE_URL')
key= os.getenv('SUPABASE_KEY')
supabase: Client = create_client(url, key)

class Web3CareerFetcher:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup Chrome options
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-software-rasterizer')
        
        self.logger.info("Initializing Web3CareerFetcher with headless Chrome")
        
        try:
            # Get Chrome version
            chrome_version = self.get_chrome_version()
            self.logger.info(f"Chrome version: {chrome_version}")
            
            # Always use webdriver_manager to get the correct ChromeDriver version
            self.logger.info("Using webdriver_manager to install matching ChromeDriver")
            try:
                driver_path = ChromeDriverManager().install()
                self.logger.info(f"ChromeDriver installed at: {driver_path}")
                service = Service(driver_path)
                self.logger.info("ChromeDriver setup completed")
            except Exception as e:
                self.logger.error(f"Failed to install ChromeDriver: {e}")
                raise

            self.driver = webdriver.Chrome(service=service, options=options)
            self.logger.info("Chrome driver initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            raise

    def get_chrome_version(self) -> str:
        """Get the installed Chrome version"""
        try:
            self.logger.info("Attempting to detect Chrome version...")
            # macOS
            if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
                self.logger.info("Detected macOS Chrome installation")
                output = subprocess.check_output([
                    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                    "--version"
                ]).decode()
                version = output.strip().split()[-1]
                self.logger.info(f"macOS Chrome version: {version}")
                return version
            # Linux
            elif os.path.exists("/usr/bin/google-chrome"):
                self.logger.info("Detected Linux Chrome installation")
                output = subprocess.check_output(["google-chrome", "--version"]).decode()
                version = output.strip().split()[-1]
                self.logger.info(f"Linux Chrome version: {version}")
                return version
            else:
                self.logger.warning("Could not detect Chrome installation")
                return None
        except Exception as e:
            self.logger.error(f"Error getting Chrome version: {e}")
            return None

    def fetch_jobs(self, max_pages: int = 5) -> List[Dict]:
        """
        Fetch jobs from cryptojobs.com up to max_pages, saving any jobs found even if some pages fail
        """
        all_jobs = []
        jobs_per_page = []  # Track jobs found on each page
        
        for page in range(1, max_pages + 1):
            url = f"https://web3.career/?page={page}"
            self.logger.info(f"Fetching page {page}")
            page_jobs = []  # Track jobs for current page
            
            try:
                self.driver.get(url)
                self.logger.info(f"Successfully loaded URL: {url}")
                
                # Wait for job listings to load
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.table_row"))
                    )
                    self.logger.info("Job listings loaded successfully")
                except Exception as e:
                    self.logger.error(f"Timeout waiting for job listings to load: {e}")
                    continue
                
                # Let the page fully render
                time.sleep(2)
                
                # Extract job listings
                job_elements = self.driver.find_elements(By.CSS_SELECTOR, "tr.table_row")
                
                self.logger.info(f"Found {len(job_elements)} job elements on page {page}")
                
                if not job_elements:
                    self.logger.warning(f"No jobs found on page {page}, but continuing to next page")
                    continue
                
                for job in job_elements:
                    try:
                        # Skip sponsored/advertisement rows
                        if "sponsor" in job.get_attribute("id"):
                            continue
                            
                        job_id = job.get_attribute("data-jobid")
                        
                        # Extract title and company
                        title = job.find_element(By.CSS_SELECTOR, "h2.fs-6").text.strip()
                        company = job.find_element(By.CSS_SELECTOR, "h3").text.strip()
                        
                        # Extract job URL
                        job_url = job.find_element(By.CSS_SELECTOR, "h2.fs-6").find_element(By.XPATH, "..").get_attribute("href")
                        
                        # Extract posted date - get both display text and datetime attribute
                        try:
                            time_element = job.find_element(By.TAG_NAME, "time")
                            posted_date = {
                                'display': time_element.text.strip(),
                                'datetime': time_element.get_attribute("datetime")
                            }
                        except:
                            posted_date = None
                            
                        # Extract location - look for text-shadow-1px paragraph
                        try:
                            # Locate the container holding job location
                            location_elements = job.find_elements(By.CLASS_NAME, "job-location-mobile")
                            # Find all anchor elements inside
                            if location_elements:
                                location = location_elements[-1].text.strip()
                                if location.lower() == company.lower():
                                    location = None
                                else:
                                    location = location 
                                # print(location)
                            else:
                                location = None
                        except:
                            location = None
                            
                        # Extract salary - look for text-shadow-1px paragraph
                        try:
                            salary_element = job.find_element(By.CSS_SELECTOR, "td[style*='text-align: end'] p.text-shadow-1px")
                            salary = salary_element.text.strip()
                        except:
                            try:
                                # Fallback to previous method
                                salary = job.find_element(By.CLASS_NAME, "text-salary").text.strip()
                            except:
                                salary = None

                        # Extract tags/skills
                        tags = [tag.text for tag in job.find_elements(By.CSS_SELECTOR, ".my-badge a")]

                        job_data = {
                            'title': title,
                            'company': company,
                            'location': location,
                            'posted_date_display': posted_date['display'] if posted_date else None,
                            'posted_datetime': posted_date['datetime'] if posted_date else None,
                            'salary': salary,
                            'salary_amount': None,
                            'skills': tags,
                            'source': 'web3.career',
                            'job_url': job_url,
                            'job_id': job_id,
                            'ingestion_date': datetime.now().strftime('%Y-%m-%d'),
                            'estimated_salary': 'estimated_star' in job.get_attribute("outerHTML"),
                            'salary_range_min': None,
                            'salary_range_max': None,
                            'is_remote': location.lower() == 'remote' if location else False,
                        }

                        # Parse salary range if it exists
                        if salary and 'k' in salary.lower():
                            try:
                                salary_parts = salary.replace('$', '').replace('k', '').split('-')
                                if len(salary_parts) == 2:
                                    job_data['salary_range_min'] = int(salary_parts[0].strip()) * 1000
                                    job_data['salary_range_max'] = int(salary_parts[1].strip()) * 1000
                            except:
                                pass

                        page_jobs.append(job_data)
                        self.logger.debug(f"Successfully extracted job: {title}")
                        
                    except Exception as e:
                        self.logger.error(f"Error extracting job data: {e}", exc_info=True)
                
                # Add the page's jobs to the main list
                all_jobs.extend(page_jobs)
                jobs_per_page.append(len(page_jobs))
                self.logger.info(f"Completed page {page}, extracted {len(page_jobs)} jobs")
                
            except Exception as e:
                self.logger.error(f"Error processing page {page}: {e}")
                continue
            
            # Add delay between pages
            time.sleep(2)
        
        # Final summary
        self.logger.info("=== Fetching Summary ===")
        for i, count in enumerate(jobs_per_page, 1):
            self.logger.info(f"Page {i}: {count} jobs")
        self.logger.info(f"Total jobs collected: {len(all_jobs)}")
        return all_jobs
    
    
    def cleanup(self):
        """
        Clean up resources
        """
        self.driver.quit()


def main():

    parser = argparse.ArgumentParser(description='Fetch jobs from web3.career')
    parser.add_argument('--max_pages', type=int, default=1, help='Maximum number of pages to fetch')
    args = parser.parse_args()
    
    fetcher = Web3CareerFetcher()
    try:
        max_pages = args.max_pages
        jobs = fetcher.fetch_jobs(max_pages)
        # Convert jobs to JSON string
        jobs_json = json.dumps(jobs).encode('utf-8')

        filename = 'web3career.json' + datetime.now().strftime('%Y-%m-%d')
        
        # Upload to Supabase storage 
        try:
            response = supabase.storage.from_('jobs-raw').upload(
                filename, 
                jobs_json, 
                {'upsert': 'true'}
            )
            fetcher.logger.info(f"Successfully uploaded data to Supabase storage: {response}")
        except Exception as upload_error:
            fetcher.logger.error(f"Error uploading to Supabase: {upload_error}")

    except Exception as e:
        fetcher.logger.error(f"Error fetching jobs: {e}")
    finally:
        fetcher.cleanup()

if __name__ == "__main__":
    main() 
