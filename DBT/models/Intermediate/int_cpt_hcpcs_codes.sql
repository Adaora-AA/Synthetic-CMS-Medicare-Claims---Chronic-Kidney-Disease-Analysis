{{ config(materialized='table') }}

SELECT DISTINCT 
    cpt_hcpcs_code,
    long_desc,
    short_desc,
    betos_code,
    betos_code_desc
FROM {{ ref('cpt_hcpcs_codes') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY cpt_hcpcs_code ORDER BY cpt_hcpcs_code) = 1