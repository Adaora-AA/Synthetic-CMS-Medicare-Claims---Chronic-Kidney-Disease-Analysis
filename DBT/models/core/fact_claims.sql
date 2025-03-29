{{
    config(
        materialized='table'
    )
}}

WITH all_claims AS (
    SELECT
        bene_id,
        clm_id,
        clm_thru_dt  AS year,
        prncpal_dgns_cd,
        prncpal_dgns_code_desc,
        hcpcs_cd,
        hcpcs_cd_desc,
        clm_pmt_amt,
        'carrier' AS provider_type,
        prvdr_state
    FROM {{ ref('fact_carrier') }}
    

    UNION ALL 

    SELECT
        bene_id,
        clm_id,
        clm_thru_dt  AS year,
        prncpal_dgns_cd,
        prncpal_dgns_code_desc,
        hcpcs_cd,
        hcpcs_cd_desc,
        clm_pmt_amt,
        'dme' AS provider_type,
        prvdr_state
    FROM {{ ref('fact_dme') }}
    

    UNION ALL

    SELECT
        bene_id,
        clm_id,
        clm_thru_dt  AS year,
        prncpal_dgns_cd,
        prncpal_dgns_code_desc,
        hcpcs_cd,
        hcpcs_cd_desc,
        clm_pmt_amt,
        'hha' AS provider_type,
        prvdr_state
    FROM {{ ref('fact_hha') }}
    

    UNION ALL

    SELECT
        bene_id,
        clm_id,
        nch_bene_dschrg_dt  AS year,
        prncpal_dgns_cd,
        prncpal_dgns_code_desc,
        hcpcs_cd,
        hcpcs_cd_desc,
        clm_pmt_amt,
        'hospice' AS provider_type,
        prvdr_state
    FROM {{ ref('fact_hospice') }}
    

    UNION ALL

    SELECT
        bene_id,
        clm_id,
        nch_bene_dschrg_dt  AS year,
        prncpal_dgns_cd,
        prncpal_dgns_code_desc,
        hcpcs_cd,
        hcpcs_cd_desc,
        COALESCE(clm_pmt_amt, 0) + (COALESCE(clm_pass_thru_per_diem_amt, 0) * COALESCE(CAST(clm_utlztn_day_cnt AS NUMERIC), 0))  AS clm_pmt_amt,
        'inpatient' AS provider_type,
        prvdr_state
    FROM {{ ref('fact_inpatient') }}
    

    UNION ALL

    SELECT
        bene_id,
        clm_id,
        clm_thru_dt  AS year,
        prncpal_dgns_cd,
        prncpal_dgns_code_desc,
        hcpcs_cd,
        hcpcs_cd_desc,
        clm_pmt_amt,
        'outpatient' AS provider_type,
        prvdr_state
    FROM {{ ref('fact_outpatient') }}

    UNION ALL

    SELECT
        bene_id,
        clm_id,
        nch_bene_dschrg_dt  AS year,
        prncpal_dgns_cd,
        prncpal_dgns_code_desc,
        hcpcs_cd,
        hcpcs_cd_desc,
        clm_pmt_amt,
        'snf' AS provider_type,
        prvdr_state
    FROM {{ ref('fact_snf') }}
    
)
SELECT * FROM all_claims
