import os
import sys
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

max_pages = 15

# Define job sources
job_sources = ["web3career", "cryptojobscom"]  

# Create the list of scripts to run with arguments
python_scripts = []

# Add all fetch scripts with max_pages argument
for source in job_sources:
    python_scripts.append({
        "script": f"scrape/fetch_{source}.py", 
        "args": ["--max_pages", str(max_pages)],
        "critical": True  # Mark as critical if failure should stop the pipeline
    })

# Add all clean scripts with no arguments
for source in job_sources:
    python_scripts.append({
        "script": f"scrape/clean_{source}.py",
        "args": [],
        "critical": False  # Can continue if this fails
    })

# Add all infer scripts with no arguments
for source in job_sources:
    python_scripts.append({
        "script": f"infer/infer.py",
        "args": [],
        "critical": False  # Can continue if this fails
    })

# Track overall success
success = True

# Run all scripts 
for script_info in python_scripts:
    script_name = script_info["script"]
    script_args = script_info["args"]
    is_critical = script_info.get("critical", False)
    
    command = ['python', script_name] + script_args
    logger.info(f"Running: {' '.join(command)}")
    
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Successfully finished {script_name}")
        else:
            logger.error(f"Error running {script_name}, return code: {result.returncode}")
            logger.error(f"Error output: {result.stderr}")
            
            if is_critical:
                logger.critical(f"Critical script {script_name} failed - stopping pipeline")
                success = False
                break
            else:
                logger.warning(f"Non-critical script {script_name} failed - continuing with next script")
    
    except Exception as e:
        logger.error(f"Exception while running {script_name}: {e}")
        
        if is_critical:
            logger.critical(f"Critical script {script_name} failed - stopping pipeline")
            success = False
            break
        else:
            logger.warning(f"Non-critical script {script_name} failed - continuing with next script")

if success:
    logger.info("All critical scripts completed successfully")
    sys.exit(0)
else:
    logger.error("Pipeline failed due to critical script failure")
    sys.exit(1)