{{
    config(
        materialized='table'
    )
}}

WITH kidney_claims AS (
    SELECT
        *
    FROM {{ ref('fact_claims') }}
    WHERE prncpal_dgns_cd LIKE 'N18%'
)
SELECT * FROM kidney_claims
