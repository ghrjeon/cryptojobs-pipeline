# Crypto Jobs Pipeline

This repository contains a pipeline for scraping job listings from various blockchain and cryptocurrency job boards.

## How It Works

The pipeline consists of:

1. **Fetch Scripts**: Selenium-based web scrapers that collect job listings from different sources
2. **Clean Scripts**: Data processing scripts that clean and standardize the collected data
3. **Ingest Script**: Main orchestration script that runs all fetch and clean scripts in sequence
4. **AI Analysis**: Utilizes OpenAI and scikit-learn for advanced job data processing and analysis

## Automated Scraping

This repository is configured with GitHub Actions to automatically run the job scraping and inference pipeline every day at 6:00 PM EST.

### GitHub Actions Workflow

The workflow defined in `.github/workflows/daily-job-scrape.yml`:

1. Runs on a schedule (daily at 6 PM EST)
2. Sets up Python and Chrome for Selenium-based scraping
3. Installs all required dependencies
4. Sets up environment variables from repository secrets
5. Runs the ingest script to fetch and process job data
6. Commits and pushes any changes back to the repository

## Secret Management

The following secrets must be configured in your GitHub repository settings:

- `SUPABASE_URL`: The URL of your Supabase instance
- `SUPABASE_KEY`: The API key for your Supabase instance
- `OPENAI_API_KEY`: Your OpenAI API key for AI-powered analysis

## Local Development

To run the pipeline locally:

1. Install dependencies: `pip install -r requirements.txt`
2. Create a `.env` file with your credentials:
   ```
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_supabase_key
   OPENAI_API_KEY=your_openai_api_key
   ```
3. Run the ingest script: `python ingest.py` 