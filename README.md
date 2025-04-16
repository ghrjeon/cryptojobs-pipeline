# Crypto Jobs Pipeline

Hello! This repository contains the ETL pipeline for <a href="https://github.com/ghrjeon/cryptojobs-aichat" target="_blank" rel="noopener noreferrer"> Crypto Jobs Analytics</a>. The pipeline scrapes job listings from various blockchain and cryptocurrency job boards, processes and enriches the data using ML and LLM, and creates an API endpoint for data retrieval.

Checkout the platform which uses this pipeline: https://rosalyn-cryptojobs-ai.vercel.app/

## Stacks used
<b>Data Collection</b>: Selenium, BeautifulSoup <br>
<b>Data Lake/Warehousing</b>: Supabase <br>
<b>Data Processing</b>: Python, OpenAI, Scikit-learn <br>
<b>Orchestration</b>: GitHub Actions <br>

## Pipeline and Methodology

The pipeline consists of:

📌 **Fetch Scripts**: Selenium-based web scrapers that collect job listings from different sources. <br>
📌 **Clean Scripts**: Data processing scripts that clean and standardize the collected data. <br>
📌 **Infer Script**: Utilizes embeddings, scikit-learn, and LLM for job data processing and text inference. <br>
📌 **Ingest Script**: Main orchestration script that runs all fetch, clean, and infer scripts in sequence. <br>
 
GitHub Actions Workflow: pipeline runs every day at 6:00 PM EST 

# Directory Structure  
      .
      ├── .github/workflows             # Defines orchestration 
      │   └── daily-job-pipeline.yml
      ├── scrape                        # Fetches and cleans job postings
      │   ├── fetch_cryptojobscom.py    
      │   ├── fetch_web3career.py
      │   ├── clean_web3career.py
      │   └── fetch_cryptojobscom.py  
      ├── infer                         # Data processing and inference using OpenAI & Scikit-learn
      │   └── infer.py                  
      ├── ingest.py                     # Script to run all pipeline components            
      └── requirements.txt              # Dependencies 


# Requirements 
- Python 3.11+
- Supabase 1.2.0
- Scikit-Learn 1.6.1
- OpenAI 1.65.4
- Pandas 1.5.3
- Numpy 1.24.4
- API Keys (PandasAI, OpenAI, Supabase) <br>
  

