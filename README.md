# Synthetic CMS Medicare Claims Dashboard

This project is a capstone demonstration for a Data Engineering initiative that leverages synthetic CMS data. The solution ingests data into Google Cloud Storage (GCS), processes and enriches it using dbt, loads it into Google BigQuery, and visualizes key insights via a Looker Studio dashboard. The focus is on analyzing the cost of claims paid over 10 years for beneficiaries with chronic kidney disease.

---

## Project Overview

- **Data Source:**  
  - **Synthetic Data:** The datasets are realistic-but-not-real. They mimic the statistical properties of actual CMS claims data while ensuring beneficiary privacy.
  - **Purpose:** Designed to help users familiarize themselves with CMS claims data without privacy restrictions, though the synthetic data has limited inferential research value.
  - **Design:** The data follows the CMS Research Identifiable File (RIF) format and represents enrollment and claims for 8,671 Medicare beneficiaries (ages 0–110).
  - **Further Information:** For more details on the synthetic data creation process, see the [Synthetic Medicare Claims User Guide](https://data.cms.gov/collection/synthetic-medicare-enrollment-fee-for-service-claims-and-prescription-drug-event).

- **Objective:**  
  Analyze trends in claim costs for beneficiaries with chronic kidney disease over a decade by:
  - Extracting and ingesting synthetic CMS data.
  - Transforming and enriching data using a multi-layered dbt architecture.
  - Loading the processed data into BigQuery.
  - Visualizing insights with a Looker Studio dashboard.

- **Cloud-First Approach:**  
  The project is entirely cloud-based using Google Cloud Platform (GCP) services. Infrastructure is provisioned via Terraform, and workflow orchestration is managed by Apache Airflow.

- **Evaluation Criteria:**  
  The project excels in clear problem description, robust cloud implementation (using IaC), automated end-to-end data ingestion and transformation, optimized BigQuery tables, modular dbt transformations, a comprehensive Looker Studio dashboard, and reproducibility.

---

## Architecture & Workflow

### Data Ingestion & Processing

1. **Data Extraction:**  
   Selenium-based scripts scrape the CMS website for synthetic claims URLs. Downloaded CSV/ZIP files are extracted and converted to Parquet using Python (with Pandas and PyArrow).

2. **Cloud Storage & Loading:**  
   The converted files are uploaded to a designated GCS bucket. Batch pipelines then load the data into BigQuery and create external tables.

3. **Transformation with dbt:**  
   Data transformation is handled using a layered dbt architecture:
   - **Staging Models:** Clean and standardize raw data.
   - **Intermediate Models:** Enrich data by joining with reference tables (ICD-10, HCPCS, type-of-bill codes) and applying custom macros.
   - **Core Models:** Aggregate the enriched data into final, analytics-ready tables.

4. **Workflow Orchestration:**  
   An Apache Airflow DAG orchestrates the overall ETL process, while Terraform scripts provision the necessary GCP resources.

5. **Visualization:**  
   Final data tables in BigQuery serve as the source for the Looker Studio dashboard, which provides interactive visualizations of claim cost trends.

---

## dbt Transformation & Modeling

### Intermediate Models

Intermediate models enrich data from the staging layer by:
- **Joining with Reference Data:**  
  Each model (e.g., `int_carrier`, `int_dme`, `int_hha`, `int_hospice`, `int_inpatient`, `int_outpatient`, `int_snf`) joins its staging table with lookup tables (ICD-10, HCPCS, type-of-bill codes) to convert raw codes into human-readable descriptions.
- **Applying Custom Macros:**  
  Macros such as `nch_clm_type_description`, `state_description`, `line_cms_type_srvc_cd_description`, and `ptnt_dschrg_stus_cd_desc` generate descriptive labels for claim types, state names, service types, and patient discharge statuses.
- **Handling Data Anomalies:**  
  Some models include logic to correct known issues (e.g., misassigned claim type codes in carrier claims).
- **Materializing as Views:**  
  All intermediate models are materialized as views for dynamic, on-demand enrichment.
- **Examples by Claim Type:**  
  - **Carrier:** Enriches carrier claims with ICD-10 and HCPCS lookups and corrects discrepancies.
  - **DME:** Uses window functions to rank HCPCS codes and select primary descriptions.
  - **HHA:** Joins HHA data with additional lookups (e.g., type-of-bill codes) to include visit counts and discharge statuses.
  - **Hospice, Inpatient, Outpatient, SNF:** Follow similar enrichment patterns to produce comprehensive descriptive outputs.

### Core Models

Core models aggregate enriched data from the intermediate layer into final, analytics-ready tables that serve as the single source of truth for reporting and visualization.

#### Beneficiary Core Model
- **Aggregation:** Combines beneficiary data from multiple staging tables (2015–2025) using `UNION ALL`.
- **Deduplication:** Uses `ROW_NUMBER()` partitioned by beneficiary ID (ordered by enrollment year descending) to select the most recent record per beneficiary.
- **Output:** Produces a single record per beneficiary with key demographic and coverage details (using `COALESCE` for defaults).

#### Carrier Core Model
- **Source:** Derived from the enriched intermediate carrier model (`int_carrier`).
- **Key Fields:** Beneficiary/claim identifiers, claim dates, payment amounts, diagnosis codes (with descriptive labels), procedure details, and provider information.
- **Purpose:** Provides a unified view of carrier claims for detailed analysis and dashboard integration.

#### DME Core Model
- **Source:** Built from the intermediate DME model (`int_dme`).
- **Key Fields:** Includes beneficiary/claim identifiers, claim dates, payment amounts, and enriched diagnosis/procedure details.
- **Purpose:** Supports trend analysis and reporting for DME claims.

#### HHA Core Model
- **Source:** Derived from the intermediate HHA model (`int_hha`).
- **Key Fields:** Extracts beneficiary/claim identifiers, claim dates, provider details, and HHA-specific information (e.g., visit counts, admission dates, patient discharge statuses) enriched with lookup values.
- **Purpose:** Provides a structured view of HHA claims for downstream analysis.

#### Hospice Core Model
- **Source:** Built from the intermediate Hospice model (`int_hospice`).
- **Key Fields:** Includes beneficiary/claim identifiers, claim dates, payment amounts, provider details, and hospice-specific fields (e.g., start dates, period counts, discharge dates) with enriched diagnosis and facility data.
- **Purpose:** Delivers a clean, unified view of hospice claims for in-depth analysis and reporting.

#### Inpatient Core Model
- **Source:** Derived from the intermediate inpatient model (`int_inpatient`).
- **Key Fields:** Consolidates beneficiary/claim identifiers, admission details, payment amounts, detailed diagnosis (with descriptive labels), procedure codes, and utilization/discharge metrics.
- **Purpose:** Supports comprehensive trend analysis and reporting for inpatient claims.

#### Outpatient Core Model
- **Source:** Built from the intermediate outpatient model (`int_outpatient`).
- **Key Fields:** Selects beneficiary/claim identifiers, claim dates, payment amounts, provider details, and outpatient-specific metrics (e.g., discharge status, procedure codes, revenue/discount metrics) enriched with reference lookups.
- **Purpose:** Provides a comprehensive view of outpatient claims for detailed analysis and dashboarding.

#### SNF Core Model
- **Source:** Derived from the enriched intermediate SNF model (`int_snf`).
- **Key Fields:** Aggregates SNF data with beneficiary/claim identifiers, claim dates, payment amounts, provider information, and SNF-specific details (e.g., utilization days, diagnosis, and procedure information).
- **Purpose:** Offers a complete view of Skilled Nursing Facility claims for analysis and reporting.

#### Aggregated Claims (Fact) Model
- **Purpose:** Combines data from all core claim models into a single consolidated table for cross-provider analyses.
- **Features:**  
  - Uses `UNION ALL` to merge claims from carrier, DME, HHA, hospice, inpatient, outpatient, and SNF models.
  - Standardizes fields such as beneficiary ID, claim ID, claim end/discharge dates (as "year"), diagnosis and procedure codes (with descriptions), and claim payment amounts (with adjustments for inpatient claims).
  - Adds a hardcoded provider type for segmentation and retains provider state information.

#### Kidney Claims Fact Model
- **Purpose:** Filters the aggregated claims data to focus on kidney-related claims by selecting records where the principal diagnosis code starts with `'N18'`.
- **Usage:** Enables targeted analysis of cost trends and claim volumes for chronic kidney disease.

---

## Technology Stack

- **Data Scraping & ETL:** Python, Selenium, Requests, Pandas, PyArrow, and the Google Cloud Storage SDK.
- **Workflow Orchestration:** Apache Airflow.
- **Data Warehouse:** Google BigQuery.
- **Data Transformation & Modeling:** dbt (organized into staging, intermediate, and core layers).
- **Visualization:** Google Looker Studio.
- **Infrastructure as Code:** Terraform.

---

## Setup & Deployment Instructions

### Prerequisites

- A Google Cloud Platform account with APIs enabled (GCS, BigQuery, etc.).
- Terraform installed and configured.
- Apache Airflow set up (local or managed).
- A Python environment with required libraries (Selenium, Pandas, PyArrow, google-cloud-storage, etc.).
- dbt installed and configured for BigQuery.

### Step-by-Step Instructions

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/Adaora-AA/ideal-tribble.git
   cd ideal-tribble

2. **Configure Google Cloud Resources:**
- Create a GCP project.
- Set up a service account** with roles for BigQuery, GCS, and Dataproc.
- Download the service account JSON key.

3. **Update Terraform Variables:**
- Edit `terraform/variables.tf` with your credentials, project, region, dataset, and bucket names.

4. ** Provision Infrastructure:**
In the `terraform` directory, run:
   ```bash
   terraform init
   terraform plan
   terraform apply

5. **Run the ETL & Transformation Pipeline:**
- Execute the ETL scripts to scrape and ingest data into GCS.
- Trigger the data processing pipelines to load data into BigQuery.
- Run the dbt models using:
  ```bash
  dbt run
6. **Workflow Orchestration:**
- Place the provided Airflow DAG files in your Airflow DAGs folder.
- Configure Airflow to connect to your GCP environment.
- Trigger the DAG manually or on a defined schedule.

7. **Dashboard Setup:**
- Connect Looker Studio to your BigQuery dataset.
- Use the provided dashboard template or customize visualizations to analyze claim cost trends.

## Visualization:
- **Access the Looker Studio dashboard here:** [Looker Studio Dashboard](https://lookerstudio.google.com/reporting/bc4003c1-f55e-4b10-a117-d3ee14734119)
- Screenshot previews (`dashboard_1.png` and `dashboard_2.png`) are available in the repository.

## Reproducibility & Future Enhancements

### Reproducibility:
The project employs automated tools (Terraform, Airflow, and dbt) to ensure a fully reproducible environment. Version-controlled transformations and IaC scripts allow easy replication of the entire pipeline.

### Future Enhancements:
- **Data Integration:** Incorporate additional data sources or real-time feeds.
- **Enhanced Visualizations:** Expand dashboard capabilities with more granular metrics.
- **Pipeline Optimization:** Explore stream-based ingestion for near-real-time processing.
- **CI/CD Integration:** Implement automated testing and deployment pipelines for continuous improvement.
