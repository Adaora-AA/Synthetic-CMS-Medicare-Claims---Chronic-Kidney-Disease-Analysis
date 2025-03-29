import logging
from google.cloud import storage
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Global Settings
PROJECT_ID = "adaora"
BUCKET_NAME = "adaoraah-terra-bucket"
BIGQUERY_DATASET = "cms_data"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def list_subdirectories(bucket_name, prefix):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix, delimiter="/")
    subdirectories = set()

    for _ in blobs:
        pass
    for prefix in blobs.prefixes:
        subdirectories.add(prefix.strip("/").split("/")[-1])
    return list(subdirectories)


def execute_bq_query(hook, query):
    try:
        hook.run_query(query)
        logger.info("Query executed successfully.")
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


def get_columns_in_table(hook, project_id, dataset_id, table_name):
    query = f"""
    SELECT column_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}_ext'
    """
    results = hook.get_pandas_df(query)
    return results['column_name'].tolist()


def get_unique_id_expr(hook, project_id, dataset_id, table_name, dataset_type):
    if dataset_type == "beneficiaries":
        # Beneficiaries unique ID snippet 
        unique_id_expr = """MD5(CONCAT(
                        COALESCE(CAST(BENE_ID AS STRING), ""),
                        COALESCE(CAST(BENE_BIRTH_DT AS STRING), ""),
                        COALESCE(CAST(SEX_IDENT_CD AS STRING), ""),
                        COALESCE(CAST(BENE_RACE_CD AS STRING), ""),
                        COALESCE(CAST(BENE_DEATH_DT AS STRING), ""),
                        COALESCE(CAST(BENE_ENROLLMT_REF_YR AS STRING), ""),
                        COALESCE(CAST(COVSTART AS STRING), "")
                    )) AS unique_row_id"""
    elif dataset_type == "claims":
        # For claims unique ID snippet, dynamically determine the column name for line number
        columns = get_columns_in_table(hook, project_id, dataset_id, table_name)
        line_num_col = "LINE_NUM" if "LINE_NUM" in columns else "CLM_LINE_NUM"
        unique_id_expr = f"""MD5(CONCAT(
                        COALESCE(CAST(BENE_ID AS STRING), ""),
                        COALESCE(CAST(CLM_ID AS STRING), ""),
                        COALESCE(CAST(CLM_FROM_DT AS STRING), ""),
                        COALESCE(CAST(CLM_THRU_DT AS STRING), ""),
                        COALESCE(CAST({line_num_col} AS STRING), ""),
                        COALESCE(CAST(CLM_PMT_AMT AS STRING), "")
                    )) AS unique_row_id"""
    else:
        raise ValueError("Invalid dataset_type provided. Must be 'beneficiaries' or 'claims'.")
    return unique_id_expr

def create_bq_tables_from_gcs(dataset_type, project_id=PROJECT_ID, dataset_id=BIGQUERY_DATASET, bucket=BUCKET_NAME, bucket_path=None):
    # CHANGED: Automatically set bucket_path based on dataset_type.
    if bucket_path is None:
        if dataset_type == "beneficiaries":
            bucket_path = "cms_data/beneficiaries/"
        elif dataset_type == "claims":
            bucket_path = "cms_data/all_ffs_claims/"
        else:
            raise ValueError("Invalid dataset_type provided. Must be 'beneficiaries' or 'claims'.")

    hook = BigQueryHook(use_legacy_sql=False)
    subdirectories = list_subdirectories(bucket, bucket_path)
    if not subdirectories:
        logger.info("No subdirectories found. Exiting.")
        return

    for subdir in subdirectories:
        logger.info(f"Processing subdirectory: {subdir}")
        bucket_url = f"gs://{bucket}/{bucket_path}{subdir}/*.parquet"
        table_name = subdir.replace("-", "_")
        print(f"Processing: {bucket_url} -> Table: {table_name}")

        # 1. Create external table 
        create_ext_table_sql = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.{table_name}_ext`
        OPTIONS (
            format = 'PARQUET',
            uris = ['{bucket_url}']
        )
        """
        execute_bq_query(hook, create_ext_table_sql)
        logger.info(f"External table '{table_name}_ext' created/updated.")

        
        unique_id_expr = get_unique_id_expr(hook, project_id, dataset_id, table_name, dataset_type)

        # 2. Create temporary table  using the unique ID expression.
        create_temp_table_sql = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_name}_tmp`
        AS
        SELECT
            {unique_id_expr},
            *
        FROM `{project_id}.{dataset_id}.{table_name}_ext`
        """
        execute_bq_query(hook, create_temp_table_sql)
        logger.info(f"Temporary table '{table_name}_tmp' created/updated.")

        # 3. Merge into main table
        create_main_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_name}`
        AS
        SELECT * FROM `{project_id}.{dataset_id}.{table_name}_tmp` temp
        WHERE 1 = 0
        """
        execute_bq_query(hook, create_main_table_sql)
        logger.info(f"Main table '{table_name}' created successfully.")

        insert_into_main_table_sql = f"""
        INSERT INTO `{project_id}.{dataset_id}.{table_name}`
        SELECT temp.*
        FROM `{project_id}.{dataset_id}.{table_name}_tmp` temp
        WHERE NOT EXISTS (
            SELECT 1
            FROM `{project_id}.{dataset_id}.{table_name}` main
            WHERE main.unique_row_id = temp.unique_row_id
        )
        """
        execute_bq_query(hook, insert_into_main_table_sql)
        logger.info(f"Data merged into '{table_name}' successfully.")


def main():
    # Process beneficiaries data
    create_bq_tables_from_gcs(dataset_type="beneficiaries", bucket_path="cms_data/beneficiaries/")
    # Process claims data
    create_bq_tables_from_gcs(dataset_type="claims", bucket_path="cms_data/all_ffs_claims/")

if __name__ == "__main__":
    main()
