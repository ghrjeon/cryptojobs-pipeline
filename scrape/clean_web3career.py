import json
import pandas as pd
import os
import ast
from supabase import create_client, Client
from dotenv import load_dotenv
import numpy as np

load_dotenv()

url= os.getenv('SUPABASE_URL')
key= os.getenv('SUPABASE_KEY')
supabase: Client = create_client(url, key)


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

def clean_salary_columns(df):
    # Ensure numeric columns
    df["salary_range_min"] = pd.to_numeric(df["salary_range_min"], errors="coerce")
    df["salary_range_max"] = pd.to_numeric(df["salary_range_max"], errors="coerce")

    # Compute salary_amount efficiently
    df["salary_amount"] = np.where(
        df["salary_range_min"].notna() & df["salary_range_max"].notna(), 
        ((df["salary_range_min"] + df["salary_range_max"]) / 2),  # If both min & max exist, take the average
        df["salary_range_min"].fillna(df["salary_range_max"])  # Otherwise, take the existing value
    )  # Convert final output to integer
    
    df["salary_amount"] = df["salary_amount"].astype("Int64")
    
    return df


def clean_job_data(df):
    df = clean_skills(df)
    df = clean_salary_columns(df)
    df['title'] = df['title'].str.replace('"', '', regex=False)
    df['title'] = df['title'].str.replace("'", '', regex=False)

    df['posted_datetime'] = pd.to_datetime(df['posted_datetime']).dt.strftime('%Y-%m-%d')
    df['ingestion_date'] = pd.to_datetime(df['ingestion_date']).dt.strftime('%Y-%m-%d')

    df = df.drop_duplicates(subset=['job_id'])

    df = df[['title', 'company', 'location', 'salary_amount', 'skills', 'source', 'job_url', 
             'job_id', 'posted_datetime', 'is_remote', 'ingestion_date']]
    return df


def main():

    
    response = supabase.storage.from_('jobs-raw').download('web3career.json')
    jobs = json.loads(response.decode('utf-8'))
    
    
    print(f"Loaded {len(jobs)} jobs")

    # Convert to pandas DataFrame
    df = pd.DataFrame(jobs)
    df = clean_job_data(df)

    # Upload to Supabase
    supabase.table("web3career").upsert(df.to_dict(orient="records")).execute()
    print("Uploaded to Supabase")

if __name__ == "__main__":
    main() 