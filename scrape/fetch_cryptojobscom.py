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

class CryptoJobsComFetcher:
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
        
        self.logger.info("Initializing CryptoJobsComFetcher with headless Chrome")
        
        try:
            # Get Chrome version
            chrome_version = self.get_chrome_version()
            self.logger.info(f"Chrome version: {chrome_version}")
            
            try:
                # Check if we're in GitHub Actions environment
                if 'GITHUB_ACTIONS' in os.environ:
                    # Use ChromeDriver installed by the GitHub Action
                    self.logger.info("Running in GitHub Actions, using pre-installed ChromeDriver")
                    service = Service('chromedriver')
                else:
                    # Local development - install specific ChromeDriver version
                    if os.path.exists("/usr/local/bin/chromedriver"):
                        self.logger.info("Removing old ChromeDriver")
                        os.system("rm /usr/local/bin/chromedriver")
                    
                    self.logger.info("Installing specific ChromeDriver version")
                    service = Service(ChromeDriverManager(version="114.0.5735.90").install())
                
                self.logger.info("ChromeDriver setup completed")
                
            except Exception as e:
                self.logger.error(f"Failed to setup ChromeDriver: {e}")
                raise

            self.driver = webdriver.Chrome(service=service, options=options)
            self.logger.info("Chrome driver initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            raise

    def get_chrome_version(self) -> str:
        """Get the installed Chrome version"""
        try:
            # macOS
            if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
                output = subprocess.check_output([
                    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                    "--version"
                ]).decode()
                return output.strip().split()[-1]
            # Linux
            elif os.path.exists("/usr/bin/google-chrome"):
                output = subprocess.check_output(["google-chrome", "--version"]).decode()
                return output.strip().split()[-1]
            else:
                self.logger.warning("Could not detect Chrome version")
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
            url = f"https://www.cryptojobs.com/jobs?sort_by=posted_at&sort_order=desc&page={page}"
            self.logger.info(f"Fetching page {page}")
            page_jobs = []  # Track jobs for current page
            
            try:
                self.driver.get(url)
                self.logger.info(f"Successfully loaded URL: {url}")
                
                # Wait for job listings to load
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "new-box"))
                    )
                    self.logger.info("Job listings loaded successfully")
                except Exception as e:
                    self.logger.error(f"Timeout waiting for job listings to load: {e}")
                    continue
                
                # Let the page fully render
                time.sleep(2)
                
                # Extract job listings
                job_elements = self.driver.find_elements(By.CLASS_NAME, "new-box")
                
                # Add count logging
                self.logger.info(f"Found {len(job_elements)} job elements on page {page}")
                
                if not job_elements:
                    self.logger.warning(f"No jobs found on page {page}, but continuing to next page")
                    continue
                
                for job in job_elements:
                    try:
                        # Get the article element that contains job details
                        article = job.find_element(By.TAG_NAME, "article")
                        details = article.find_element(By.CLASS_NAME, "details")
                        
                        # Extract title and URL
                        title_element = details.find_element(By.CSS_SELECTOR, "h2 a")
                        title = title_element.text
                        job_url = title_element.get_attribute("href")
                        
                        # Extract company
                        company = details.find_element(By.CSS_SELECTOR, "ul.info li b").text
                        
                        # Extract location if it exists
                        try:
                            location = details.find_element(By.CSS_SELECTOR, "i.la-map-marker").find_element(By.XPATH, "..").text.strip()
                        except:
                            location = None

                        try:
                            remote = details.find_element(By.CSS_SELECTOR, "i.la-clock").find_element(By.XPATH, "..").find_element(By.TAG_NAME, "a").text.strip()
                        except:
                            remote = None

                        # Extract job type
                        job_type = details.find_element(By.CSS_SELECTOR, "ul.other li:first-child a").text.strip()
                        
                        # Extract job function
                        try:
                            job_function = details.find_element(By.CSS_SELECTOR, "ul.other li:nth-child(2) a").text.strip()
                        except:
                            job_function = None

                        # Extract salary if it exists
                        try:
                            salary = details.find_element(By.CSS_SELECTOR, "ul.other li i.la-wallet").find_element(By.XPATH, "..").text.strip()
                        except:
                            salary = None

                        # Extract posted date
                        posted_date = article.find_element(By.XPATH, ".//ul[contains(@class, 'date')]//span").text.strip()

                        # Extract views and applications
                        count_elements = article.find_elements(By.CSS_SELECTOR, "ul.count li")
                        views = count_elements[0].text.strip() if len(count_elements) > 0 else "0"
                        applications = count_elements[1].text.strip() if len(count_elements) > 1 else "0"

                        # Get all tags
                        tags = [tag.text for tag in article.find_elements(By.CSS_SELECTOR, "ul.tags li a")]

                        job_data = {
                            'title': title,
                            'company': company,
                            'remote': remote,
                            'location': location,
                            'job_type': job_type,
                            'job_function': job_function,
                            'salary': salary,
                            'posted_date': posted_date,
                            'views': views,
                            'applications': applications,
                            'skills': tags,
                            'source': 'cryptojobs.com',
                            'job_url': job_url,
                            'ingestion_date': datetime.now().strftime('%Y-%m-%d'),
                            'job_id': job_url.split('-')[-1]
                        }
                        
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

    fetcher = CryptoJobsComFetcher()
    try:
        max_pages = args.max_pages
        jobs = fetcher.fetch_jobs(max_pages)
        
        # Convert jobs to JSON string
        jobs_json = json.dumps(jobs).encode('utf-8')
        
        # Upload to Supabase storage 
        try:
            response = supabase.storage.from_('jobs-raw').upload(
                'cryptojobscom.json', 
                jobs_json, 
                {'upsert': 'true'}
            )
            fetcher.logger.info(f"Successfully uploaded data to Supabase storage: {response}")
        except Exception as upload_error:
            fetcher.logger.error(f"Error uploading to Supabase: {upload_error}")
            # Fallback to local save if upload fails
            fetcher.save_to_json(jobs, 'cryptojobscom.json')

    except Exception as e:
        fetcher.logger.error(f"Error fetching jobs: {e}")
    finally:
        fetcher.cleanup()

if __name__ == "__main__":
    main() 
