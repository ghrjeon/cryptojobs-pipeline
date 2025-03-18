import os
import sys
import subprocess

max_pages = 15

# Define job sources
job_sources = ["web3career", "cryptojobscom"]  

# Create the list of scripts to run with arguments
python_scripts = []

# Add all fetch scripts with max_pages argument
for source in job_sources:
    python_scripts.append({
        "script": f"scrape/fetch_{source}.py", 
        "args": ["--max_pages", str(max_pages)]
    })

# Add all clean scripts with no arguments
for source in job_sources:
    python_scripts.append({
        "script": f"scrape/clean_{source}.py",
        "args": []
    })

# Add all infer scripts with no arguments
for source in job_sources:
    python_scripts.append({
        "script": f"infer/infer.py",
        "args": []
    })

# Run all scripts 
for script_info in python_scripts:
    script_name = script_info["script"]
    script_args = script_info["args"]
    command = ['python', script_name] + script_args
    print(f"Running: {' '.join(command)}")
    subprocess.call(command)
    print("finished", script_name)