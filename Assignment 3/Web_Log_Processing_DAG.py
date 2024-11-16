from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import re

# Set up file paths for the workflow
current_file_directory = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(current_file_directory, "output")
LOG_DIR = os.path.join(current_file_directory, "the_logs")

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# DAG configuration parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Process web server logs and extract IP addresses',
    schedule_interval='@daily',
    catchup=False
)

# Task to check if the input log file exists
scan_for_log = BashOperator(
    task_id='scan_for_log',
    bash_command=f'''
        if [ -e {os.path.join(LOG_DIR, "log.txt")} ]; then
            echo "Log file found"
        else
            echo "Log file not found"
            exit 1
        fi
    ''',
    dag=dag
)

def extract_ip_addresses(**context):
    """
    Parse the log file and extract all IP addresses into a separate file.
    Each IP address is written on a new line in the output file.
    
    Args:
        **context: Airflow context containing runtime variables
        
    Returns:
        str: Path to the file containing extracted IP addresses
        
    Raises:
        Exception: When file operations fail or log format is invalid
    """
    log_file_path = os.path.join(LOG_DIR, "log.txt")
    output_file_path = os.path.join(OUTPUT_DIR, "extracted_data.txt")
    
    # Pattern to match IPv4 addresses
    ip_pattern = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')
    
    try:
        with open(log_file_path, 'r') as log_file:
            # Find all IP addresses in each line
            ip_addresses = [
                match.group(1) 
                for line in log_file 
                for match in ip_pattern.finditer(line)
            ]
        
        # Store extracted IP addresses
        with open(output_file_path, 'w') as output_file:
            for ip in ip_addresses:
                output_file.write(f"{ip}\n")
                
        return output_file_path
        
    except Exception as e:
        print(f"Error occurred while extracting IP addresses: {str(e)}")
        raise

def transform_data(**context):
    """
    Remove specified IP address from the extracted data.
    Read the input file line by line and exclude matching IP addresses.
    
    Args:
        **context: Airflow context containing runtime variables
        
    Returns:
        str: Path to the file containing filtered IP addresses
        
    Raises:
        Exception: When file operations fail
    """
    input_file_path = os.path.join(OUTPUT_DIR, "extracted_data.txt")
    output_file_path = os.path.join(OUTPUT_DIR, "transformed_data.txt")
    ip_to_filter = "198.46.149.143"
    
    try:
        with open(input_file_path, 'r') as input_file:
            # Remove specified IP address from the list
            filtered_ips = [
                line.strip() 
                for line in input_file 
                if line.strip() != ip_to_filter
            ]
        
        with open(output_file_path, 'w') as output_file:
            for ip in filtered_ips:
                output_file.write(f"{ip}\n")
                
        return output_file_path
        
    except Exception as e:
        print(f"Error occurred while transforming data: {str(e)}")
        raise

# Task to extract IP addresses from the log file
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_ip_addresses,
    provide_context=True,
    dag=dag
)

# Task to filter out specific IP address
transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

# Task to archive the processed data
load_data = BashOperator(
    task_id='load_data',
    bash_command=f'tar -czf {os.path.join(current_file_directory, "weblog.tar")} -C {OUTPUT_DIR} transformed_data.txt',
    dag=dag
)

# Define the order of task execution
scan_for_log >> extract_data >> transform_data >> load_data