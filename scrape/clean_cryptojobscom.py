import json
import pandas as pd
import os
from datetime import datetime
import ast
from supabase import create_client, Client
from dotenv import load_dotenv
import re
import numpy as np

load_dotenv()

url= os.getenv('SUPABASE_URL')
key= os.getenv('SUPABASE_KEY')
supabase: Client = create_client(url, key)


def clean_salary_columns(df):
    # Ensure salary column is a string and handle NaN values
    df["salary"] = df["salary"].fillna("").astype(str).str.replace(",", "").str.lower()

    # Define regex pattern for extracting salary details
    pattern = r"(\d+)\s*(usd|eur|gbp|jpy)?\s*/?\s*(year|month|week|day)?"

    # Extract salary details using vectorized `.str.extract()`
    extracted = df["salary"].str.extract(pattern)

    # Convert salary amount to numeric, handling NaN values properly
    df["salary_amount"] = pd.to_numeric(extracted[0], errors="coerce")
    
    # Handle currency and period
    df["salary_currency"] = extracted[1].str.upper().fillna("Unknown")
    df["salary_period"] = extracted[2].str.lower().fillna("Unknown")

    # If all salary fields are empty, set all fields to None/null
    mask = df["salary"] == ""
    df.loc[mask, ["salary_amount", "salary_currency", "salary_period"]] = None

    # Convert to nullable integer type and handle any infinity values
    df["salary_amount"] = pd.to_numeric(df["salary_amount"], errors="coerce")  # Convert inf to NaN
    df["salary_amount"] = df["salary_amount"].astype("Int64")  # Convert to nullable integer
    
    return df


def clean_skills(df):
    def parse_skills(skills):
        if isinstance(skills, str):
            try:
                skills = ast.literal_eval(skills)
            except (SyntaxError, ValueError):
                skills = []
        return skills if isinstance(skills, list) else []

    # Apply parsing function to the entire column
    df["skills"] = df["skills"].apply(parse_skills)
    # Remove skills that start with "+"
    df["skills"] = df["skills"].apply(lambda skills: [skill for skill in skills if not skill.startswith("+")])
    
    return df

def clean_date(df):
    # Ensure ingestion_date is a valid datetime
    df["ingestion_date"] = pd.to_datetime(df["ingestion_date"], errors="coerce")
    
    # Ensure posted_date is lowercase and stripped
    df["posted_date"] = df["posted_date"].astype(str).str.lower().str.strip()
    
    # Define mappings for relative dates
    relative_dates = {
        "today": 0,
        "yesterday": 1,
        "2 days ago": 2,
        "3 days ago": 3,
        "4 days ago": 4,
        "5 days ago": 5,
        "6 days ago": 6,
        "7 days ago": 7,
        "1 week ago": 7,
        "2 weeks ago": 14,
        "3 weeks ago": 21,
        "1 month ago": 30,
        "2 months ago": 60,
        "3 months ago": 90
    }
    
    # Initialize posted_datetime column
    df["posted_datetime"] = None
    
    # Handle relative dates
    for date_text, days in relative_dates.items():
        mask = df["posted_date"] == date_text
        df.loc[mask, "posted_datetime"] = df.loc[mask, "ingestion_date"] - pd.Timedelta(days=days)
    
    # For non-relative dates, try to parse them directly
    mask = df["posted_datetime"].isna()
    df.loc[mask, "posted_datetime"] = pd.to_datetime(df.loc[mask, "posted_date"], errors="coerce")
    
    # Format dates as YYYY-MM-DD strings
    df["posted_datetime"] = pd.to_datetime(df["posted_datetime"]).dt.strftime("%Y-%m-%d")
    df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.strftime("%Y-%m-%d")
    
    return df


def clean_job_data(df):
    df = clean_skills(df)
    df = clean_salary_columns(df)
    df = clean_date(df)

    df['is_remote'] = df['remote'].str.lower() == 'remote'

    df['title'] = df['title'].replace('"', '')
    df['title'] = df['title'].replace("'", '')

    df = df.drop_duplicates(subset=['job_id','company'])

    # Select columns and handle any potential problematic values
    df = df[['title', 'company', 'location', 'salary_amount', 'skills', 'source', 'job_url', 
             'job_id', 'posted_datetime', 'is_remote', 'ingestion_date']]
    
    # Replace any infinite or NaN values with None
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)
    
    return df

def main():

    filename = 'cryptojobscom.json' + datetime.now().strftime('%Y-%m-%d')

    response = supabase.storage.from_('jobs-raw').download(filename)
    jobs = json.loads(response.decode('utf-8'))
    
    print(f"Loaded {len(jobs)} jobs")

    df = pd.DataFrame(jobs)
    df = clean_job_data(df)
    
    # Convert DataFrame to dict 
    records = df.to_dict(orient="records")

    # Upload to Supabase with cleaned records
    supabase.table("cryptojobscom").upsert(records).execute()
    print("Uploaded to Supabase")

if __name__ == "__main__":
    main() 