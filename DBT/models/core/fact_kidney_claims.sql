{{
    config(
        materialized='table'
    )
}}

WITH kidney_beneficiaries AS (
    -- Join kidney_claims with beneficiaries to bring in demographics
    SELECT
        kc.*, 
        b.race, 
        b.gender, 
        b.age_at_end_ref_yr
    FROM {{ ref('dim_kidney_claims') }} kc
    LEFT JOIN {{ ref('fact_bene') }} b ON kc.bene_id = b.bene_id --check is left join changes count because that means there are some claims with no bene_id
) 

SELECT 
    bene_id as beneficiary_id,  
    clm_id as claim_id,
    race,
    gender,
    year as discharge_date,  
    age_at_end_ref_yr as age_at_end_of_year, 
    prncpal_dgns_cd as diagnosis_code,
    prncpal_dgns_code_desc as diagnosis_code_desc,
    clm_pmt_amt as claim_payment_amount,
    provider_type,
    prvdr_state as provider_state
    
FROM kidney_beneficiaries 