import os
import requests
from google.cloud import storage  
import zipfile
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import logging
from datetime import datetime
import gc
from fetch_urls import fetch_cms_urls



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_file(url):
    file_name = url.split("/")[-1]

    try:
        response = requests.get(url, timeout=30)
    except requests.exceptions.Timeout:
        print("Request timed out.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    
    if response.status_code != 200:
        print(f"Error downloading file: {response.status_code}")
        return None
    
    with open(file_name, 'wb') as f_out:
        f_out.write(response.content)

    if file_name.endswith(".zip"):
        extract_folder = file_name.replace(".zip", "")
        os.makedirs(extract_folder, exist_ok=True)
        extracted_csv_files = []

        try:
            with zipfile.ZipFile(file_name, 'r') as zip_ref:
                zip_ref.extractall(extract_folder) 
                extracted_csv_files = [os.path.join(extract_folder, f) for f in zip_ref.namelist() if f.endswith(".csv")]
        except zipfile.BadZipFile:
            print("Error: The file is not a valid ZIP file.")
            return None
        except Exception as e:
            print(f"Error extracting ZIP: {e}")
            return None
        
        os.remove(file_name)
        return extracted_csv_files[0] if extracted_csv_files else None

    if file_name.endswith(".csv"):
        print(f"Saved CSV: {file_name}")
        return os.path.abspath(file_name)

    return None

def infer_schema(csv_file, sample_size=1000):
    sample_df = pd.read_csv(csv_file, sep="|", nrows=sample_size, low_memory=False, dtype=str)
    inferred_schema = {col: pa.string() for col in sample_df.columns}
    return pa.schema(list(inferred_schema.items())) 

def convert_to_parquet(csv_file):
    parquet_file = csv_file.replace(".csv", ".parquet")
    inferred_schema = infer_schema(csv_file)
    
    with pq.ParquetWriter(parquet_file, inferred_schema) as writer:
        chunksize = 5000
        for chunk in pd.read_csv(csv_file, sep="|", low_memory=False, chunksize=chunksize, dtype=str):
            try:
                chunk_table = pa.Table.from_pandas(chunk, schema=inferred_schema, safe=False)
                writer.write_table(chunk_table)
            except pa.lib.ArrowInvalid as e:
                logging.error(f"Schema mismatch: {e}")
                problematic_rows = chunk.applymap(lambda x: isinstance(x, str) and not x.isnumeric()).sum()
                logging.error(f"Problematic rows: {problematic_rows}")
            del chunk 
            gc.collect()
    
    logging.info(f"Parquet file saved: {parquet_file}")
    return parquet_file

def upload_to_gcs(bucket_name, object_name, parquet_file):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(parquet_file, content_type="application/octet-stream")
        print(f"File successfully uploaded to gs://{bucket_name}/{object_name}")
        os.remove(parquet_file)
    except Exception as e:
        print(f"Error uploading {parquet_file} to GCS: {e}")


def process_urls(urls, bucket_name, folder_prefix, file_prefix, execution_date):
    for url in urls:
        csv_file = download_file(url)
        if not csv_file:
            print(f"No CSV file found for {url}. Skipping.")
            continue

        base_name = os.path.basename(csv_file)
        base_name = os.path.splitext(base_name)[0]
        
        parquet_file = convert_to_parquet(csv_file)
        if not parquet_file:
            print(f"Parquet conversion failed for {csv_file}. Skipping upload.")
            continue     

        object_name = f"{folder_prefix}/{base_name}/{file_prefix}_{execution_date}.parquet" 
        upload_to_gcs(bucket_name, object_name, parquet_file)

def main():
    BUCKET_NAME = "adaoraah-terra-bucket"
    execution_date = datetime.now().strftime('%Y_%m')
    
    claims_urls, beneficiary_urls = fetch_cms_urls()

   
    process_urls(beneficiary_urls, BUCKET_NAME, "cms_data/beneficiaries", "beneficiary", execution_date)
    process_urls(claims_urls, BUCKET_NAME, "cms_data/all_ffs_claims", "claims", execution_date)

if __name__ == "__main__":
    main()
