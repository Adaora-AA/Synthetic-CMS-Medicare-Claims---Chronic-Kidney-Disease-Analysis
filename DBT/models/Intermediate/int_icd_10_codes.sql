{{ config(materialized='table') }}

SELECT DISTINCT
    ICD_Code, 
    Short_Description, 
    Flag, 
    Long_Description
FROM {{ ref('icd_10_codes') }}