import pandas as pd
from supabase import create_client, Client
from openai import OpenAI
from dotenv import load_dotenv
import os
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import ast
import re
import time


# Load environment variables
load_dotenv()

supabaseUrl = os.getenv("SUPABASE_URL")
supabaseKey = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(supabaseUrl, supabaseKey)
aiClient= OpenAI(api_key=os.getenv('OPENAI_API_KEY'))


job_sources = ["web3career", "cryptojobscom"]  

def main():

    # Get job data from supabase
    df1 = get_job_latest_data(job_sources[0])
    df2 = get_job_latest_data(job_sources[1])

    # df1.to_csv('df1.csv', index=False)
    # df2.to_csv('df2.csv', index=False)
    print("\nDataset 1 size:", len(df1))
    print("Dataset 2 size:", len(df2))

    if df1.empty and df2.empty:
        print("Both datasets empty — nothing to process")
        return pd.DataFrame()

    # Calculate job similarity
    combined_df = calculate_job_similarity(df1, df2)
    print("\nFinal combined dataset size:", len(combined_df))

    # Infer job function
    combined_df = infer_job_function(combined_df)
    print("Job functions inferred")

    # Infer location
    combined_df = infer_location(combined_df)
    print("Locations inferred")

    # Clean data
    combined_df = clean_data(combined_df)

    # Select columns to keep
    combined_df = combined_df[['title', 'job_function','company', 'location','salary_amount', 'skills', 'source', 'job_url', 'job_id', 
                               'posted_date', 'is_remote',  'ingestion_date', 'my_id']].sort_values(by='posted_date', ascending=False)

    combined_df = combined_df.drop_duplicates(subset=['my_id'], keep='last')

    # Upload to supabase
    upload_to_supabase(combined_df, "jobs_clean")
    print('Jobs uploaded to supabase')

    return combined_df


def calculate_job_similarity(df1: pd.DataFrame, df2: pd.DataFrame):

    # Reset index
    df1 = df1.reset_index(drop=True)
    df2 = df2.reset_index(drop=True)

    if df1.empty or df2.empty:
        if df1.empty and df2.empty:
            print("\nBoth datasets empty; skipping similarity")
        elif df1.empty:
            print("\nFirst dataset empty; skipping similarity")
        else:
            print("\nSecond dataset empty; skipping similarity")
        return pd.concat([df1, df2]).reset_index(drop=True)

    # Ensure all columns are strings
    df1["title"] = df1["title"].fillna("").astype(str)
    df1["company"] = df1["company"].fillna("").astype(str)
    df1["posted_datetime"] = df1["posted_datetime"].fillna("").astype(str)
    df2["title"] = df2["title"].fillna("").astype(str)
    df2["company"] = df2["company"].fillna("").astype(str)
    df2["posted_datetime"] = df2["posted_datetime"].fillna("").astype(str)

    # Combine title, company, and posted date into a single column for identification
    df1["combined"] = df1["title"] + " " + df1["company"] + " " + df1["posted_datetime"]
    df2["combined"] = df2["title"] + " " + df2["company"] + " " + df2["posted_datetime"]

    # Get embeddings for the combined columns   
    df1['embedding'] = df1['combined'].apply(get_embedding)
    df2['embedding'] = df2['combined'].apply(get_embedding)

    # Convert embeddings to numpy arrays
    df1_embeddings = np.array(df1["embedding"].tolist())
    df2_embeddings = np.array(df2["embedding"].tolist())

    # Calculate cosine similarity between embeddings
    similarity_matrix = cosine_similarity(df1_embeddings, df2_embeddings)

    threshold = 0.85
    
    # Find similar pairs above threshold
    similar_indices = np.where(similarity_matrix > threshold)
    similar_pairs = pd.DataFrame({
        'job1_index': similar_indices[0],
        'job2_index': similar_indices[1],
        'similarity': similarity_matrix[similar_indices],
        'job1_title': df1.iloc[similar_indices[0]]['title'].values,
        'job2_title': df2.iloc[similar_indices[1]]['title'].values,
        'job1_company': df1.iloc[similar_indices[0]]['company'].values,
        'job2_company': df2.iloc[similar_indices[1]]['company'].values,
        'job1_location': df1.iloc[similar_indices[0]]['location'].values,
        'job2_location': df2.iloc[similar_indices[1]]['location'].values
    })
    
    if not similar_pairs.empty:
        # Sort by similarity
        similar_pairs = similar_pairs.sort_values('similarity', ascending=False)
        
        # Create masks for location presence
        job1_has_location = similar_pairs['job1_location'].notna()
        job2_has_location = similar_pairs['job2_location'].notna()
        
        # Create masks for different conditions
        # Keep job2 if it has location and job1 does not
        keep_job2 = (job1_has_location == False) & (job2_has_location == True)
        # Keep job1 if it does not have location or job2 does not have location
        keep_job1 = ~keep_job2 
        
        # Get unique indices to drop from both dataframes
        indices_to_drop_df1 = set(similar_pairs[keep_job2]['job1_index'])
        indices_to_drop_df2 = set(similar_pairs[keep_job1]['job2_index'])
        
        # Remove duplicates from both dataframes
        df1_unique = df1.drop(indices_to_drop_df1)
        df2_unique = df2.drop(indices_to_drop_df2)
        
        print(f"\nFound {len(similar_pairs)} pairs of similar jobs above threshold {threshold}")
        print(f"Removed {len(indices_to_drop_df1)} jobs from first dataset")
        print(f"Removed {len(indices_to_drop_df2)} jobs from second dataset")
        print("\nSimilar job pairs:")
        
        # Combine unique jobs from both sources
        combined_df = pd.concat([df1_unique, df2_unique]).reset_index(drop=True)
    
    else:
        print("\nNo similar jobs found above threshold")
        # Combine all jobs from both sources
        combined_df = pd.concat([df1, df2]).reset_index(drop=True)
    
    return combined_df


def get_embedding(text, retries=3):
    for attempt in range(retries):
        try:
            response = aiClient.embeddings.create(
                model="text-embedding-3-small",
                input=text,
                encoding_format="float"
            )
            return response.data[0].embedding
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"Embedding API error (attempt {attempt + 1}/{retries}): {e}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"Embedding API failed after {retries} attempts: {e}")
                raise


def infer_location(df):

    df['location_country'] = df['location'].apply(
        lambda x: 'Remote' if isinstance(x, str) and 'remote' in x.lower() 
        else 'Unknown' if pd.isna(x) or x == '' 
        else None
    )
    # Override to Remote when is_remote flag is set, but keep existing values otherwise
    df.loc[df['is_remote'] == True, 'location_country'] = 'Remote'
    
    # Get unique locations that need AI processing
    locations_to_process = df[df['location_country'].isna()]['location'].unique()
    
    if len(locations_to_process) > 0:
        # Prepare batch of locations 
        locations_str = "\n".join([f"- {loc}" for loc in locations_to_process])
        prompt = f"""
        For each location, return only its country name.
        Use standard country names (e.g., "United States" not "USA").
        If it's a city/state, return its country.
        If it's a country, return the country.
        If it's a continent, return the continent.
        If it's unidentified, return "Unknown".
        
        Locations:
        {locations_str}
        
        Format: location -> country
        """
        
        # print(locations_str)
        response = aiClient.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        # print(response.choices[0].message.content)

        # Parse response and create mapping
        country_map = {}
        for line in response.choices[0].message.content.strip().split('\n'):
            if '->' in line:
                loc, country = line.split('->')
                country_map[loc.strip().replace("- ", "")] = country.strip()
        # print(country_map)
        
        # Update DataFrame with inferred countries
        df.loc[df['location_country'].isna(), 'location_country'] = \
            df.loc[df['location_country'].isna(), 'location'].map(country_map)
    
    # Fill any remaining NAs with Unknown
    df['location_country'] = df['location_country'].fillna('Unknown')
    print('Locations inferred')
    return df

    
def infer_job_function(df):
    df['job_function'] = "Unknown"
    df['title'] = df['title'].fillna("").astype(str)
    df = df.reset_index(drop=True)

    # Resorting to key words matching for now because LLM is insufficient and needs some fine tuning! 
    # First use key words matching to infer job function
    
    data_keywords = ["Data", "Analytics", "Data Scientist", "Data Engineer", "Data Analyst", 
                     "Analytics Engineer", "Quantitative Researcher", "Business Intelligence"]
    data_keywords = set(k.lower() for k in data_keywords)

    engineering_keywords = ["Engineer", "developer", "DevOps", "Software", "Frontend", 
                            "Backend", "Full Stack", "Blockchain", "Smart Contract",
                            "solidity", "rust", "blockchain developer",
                            "blockchain engineer",
                            "Product", "Product Manager", "Product Owner", "Product Manager/Owner",
                            "Research Engineer", "QA Engineer", 
                            "Project Manager", "Technical Lead","Dev", "Cryptograph" ,
                            "qa", "system"]
    engineering_keywords = set(k.lower() for k in engineering_keywords)

    business_keywords = ["Strategy", "Operations", "Sales", "Marketing",
                        "Partnership", "Community", "Content", "Social Media",
                         "Customer Success", "Account Management", "Legal", "Compliance",
                         "HR", "People Operations", "Finance", "Accounting", "Administrative",
                         "Support", "Officer","Solutions", "Copywriter", "account",
                         "finance", "general", "executive", "financial", "tax", "treasury",
                         "payroll", "writer", "Event", "recruit", "representative",
                         "customer","auditor", "Business Development"]
    business_keywords = set(k.lower() for k in business_keywords)
    
    design_keywords = ["Designer", "Art", "Creative", "UI/UX", "Graphic",
                       "Motion", "Visual",  "Animation", "3D", "Video"]
    design_keywords = set(k.lower() for k in design_keywords)

    
    for index, row in df.iterrows():
        # Clean the title 
        title = row['title'].lower()
        title = re.sub(r"\s*\(.*?\)", "", title).strip()
        title = title.replace("(", "").replace(")", "").strip()
        title = title.replace("/", "").strip()
        title = title.split(",")[0]
        title = title.split("-")[0]

        # Match in priority order: Data -> Engineering -> Business -> Art
        if any(keyword in title for keyword in data_keywords):
            df.loc[index, 'job_function'] = "Data and Analytics"
        elif any(keyword in title for keyword in engineering_keywords):
            df.loc[index, 'job_function'] = "Engineering, Product, and Research"
        elif any(keyword in title for keyword in business_keywords):
            df.loc[index, 'job_function'] = "Business, Strategy, and Operations"
        elif any(keyword in title for keyword in design_keywords):
            df.loc[index, 'job_function'] = "Design, Art, and Creative"
        else:
            df.loc[index, 'job_function'] = "Unknown"

    jobs_to_process = df[df['job_function'] == "Unknown"]
    jobs_to_process = jobs_to_process.reset_index(drop=True)
    print(f"Jobs to process with AI: {len(jobs_to_process)}")

    for index, row in jobs_to_process.iterrows():
        # Clean the title 
        title = row['title'].lower()
        title = re.sub(r"\s*\(.*?\)", "", title).strip()
        title = title.replace("(", "").replace(")", "").strip()
        title = title.replace("/", "").strip()
        title = title.split(",")[0]
        title = title.split("-")[0]

        response = aiClient.chat.completions.create(
            model="ft:gpt-3.5-turbo-0125:personal::BDiM7gWS",
            messages=[
                {"role": "user", "content": f"Job Title: {title} \n\nJob Function:"}
            ]
        )
        jobs_to_process.loc[index, 'job_function'] = response.choices[0].message.content.strip()

    # Combine the processed jobs with the rest of the dataframe
    df = pd.concat([df[df['job_function'] != "Unknown"], jobs_to_process]).reset_index(drop=True)
    print("Unknown:", len(df[df['job_function'] == "Unknown"]))

    job_function_list = ['Engineering, Product, and Research', 'Business, Strategy, and Operations', 'Data and Analytics', 'Design, Art, and Creative']
    df['job_function'] = df['job_function'].apply(lambda x: x if x in job_function_list else 'Unknown')
    # df = df[df['job_function'] != 'Unknown']

    return df

def clean_data(df):

    # Convert string representation of a list into an actual list
    df['skills'] = df['skills'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x)

    # Add ingestion date and job ids
    df['ingestion_date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
    df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce').astype("Int64")
    df['posted_datetime'] = pd.to_datetime(df['posted_datetime'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['my_id'] = df['posted_datetime'].astype(str) + "-" + df['job_id'].astype(str)

    # Replace infinities with None
    df['salary_amount'] = df['salary_amount'].replace([np.inf, -np.inf], pd.NA)  
    df['salary_amount'] = df['salary_amount'].astype("Int64")

    # Replace location with location_country
    df.drop(columns=['location'], inplace=True)
    df.rename(columns={'location_country': 'location'}, inplace=True)

    # Rename columns
    df.rename(columns={'posted_datetime': 'posted_date'}, inplace=True)
    
    return df

def _json_safe_value(value):
    if value is None:
        return None
    if isinstance(value, (list, dict, str, int, bool)):
        return value
    if isinstance(value, float):
        return None if np.isnan(value) else value
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        ts = pd.Timestamp(value)
        return None if pd.isna(ts) else ts.strftime('%Y-%m-%d')
    if isinstance(value, (np.integer, np.floating)):
        return None if pd.isna(value) else value.item()
    if isinstance(value, np.bool_):
        return bool(value)
    if pd.api.types.is_scalar(value) and pd.isna(value):
        return None
    return value


def _json_safe_records(df: pd.DataFrame) -> list:
    records = df.to_dict(orient="records")
    for record in records:
        for key in record:
            record[key] = _json_safe_value(record[key])
    return records


def upload_to_supabase(df, table_name: str):
    print(f"dataframe size: {len(df)}")
    df = df.drop_duplicates(subset=['my_id'], keep='last')
    print(f"Dropped duplicates, {len(df)} jobs left")

    if df.empty:
        print("No records to upload")
        return None

    records = _json_safe_records(df)

    BATCH_SIZE = 100
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        supabase.table(table_name).upsert(batch, on_conflict="my_id").execute()
        print(f"  Uploaded batch {i // BATCH_SIZE + 1} ({len(batch)} records)")

    return None

def get_job_latest_data(table_name: str) -> pd.DataFrame:
    thirty_days_ago = (pd.Timestamp.now() - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
    response = (
        supabase.table(table_name)
        .select('*')
        .gte('ingestion_date', thirty_days_ago)  
        .execute()
    )
    df = pd.DataFrame(response.data)

    if df.empty or 'ingestion_date' not in df.columns:
        print(f"No data found for {table_name}")
        return pd.DataFrame()

    df['ingestion_date'] = pd.to_datetime(df['ingestion_date'], errors='coerce')
    latest_ingestion = df['ingestion_date'].max()
    print(f"Latest ingestion date: {latest_ingestion}")

    # Incremental: only jobs from the most recent scrape batch
    df = df[df['ingestion_date'] == latest_ingestion]
    print(f"Rows from latest ingestion batch: {len(df)}")

    if 'posted_datetime' in df.columns:
        df['posted_datetime'] = pd.to_datetime(df['posted_datetime'], errors='coerce')
        df = df.sort_values(by='posted_datetime', ascending=False)

    return df

if __name__ == "__main__":
    main()
