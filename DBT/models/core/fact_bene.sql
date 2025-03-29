{{
    config(
        materialized='table'
    )
}}

WITH beneficiaries_data AS (
    SELECT * FROM {{ ref('stg_bene_2015') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2016') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2017') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2018') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2019') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2020') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2021') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2022') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2023') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2024') }} 
    UNION ALL SELECT * FROM {{ ref('stg_bene_2025') }}
), 

deduplicated_bene AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bene_id ORDER BY enrollmnt_year DESC) AS rn
    FROM beneficiaries_data
)

SELECT
    bene_id,
    coalesce(state_code, 'n/a') as state_code,
    coalesce(sex_cd, 'n/a') as sex_cd,
    coalesce(gender, 'n/a') as gender,
    coalesce(race_cd, 'n/a') as race_cd,
    coalesce(race, 'n/a') as race,
    age_at_end_ref_yr,
    coalesce(rti_race_cd, 'n/a') as rti_race_cd,
    bene_birth_dt,
    bene_death_dt,
    covstart,
    enrollmnt_year as enrollmnt_ref_year,
    valid_death_dt_sw,
    coalesce(entlmt_rsn_orig, 'n/a') as entlmt_rsn_orig,
    coalesce(entlmt_rsn_curr, 'n/a') as entlmt_rsn_curr,
    coalesce(county_cd, 'n/a') as county_cd,
    coalesce(zip_cd, 'n/a') as zip_cd,
    coalesce(esrd_ind, 'n/a') as esrd_ind,
    bene_pta_trmntn_cd,
    bene_ptb_trmntn_cd,
    bene_hi_cvrage_tot_mons,
    bene_smi_cvrage_tot_mons,
    bene_state_buyin_tot_mons,
    bene_hmo_cvrage_tot_mons,
    rds_cvrg_mons,
    coalesce(enrl_src, 'n/a') as enrl_src,
    coalesce(sample_grp, 'n/a') as sample_grp,
    coalesce(enhanced_five_percent_flag, 'n/a') as enhanced_five_percent_flag,
    coalesce(crnt_bic_cd, 'n/a') as crnt_bic_cd,
    dual_elgbl_mons,
    {% for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'] %}
        coalesce(state_cnty_fips_cd_{{ month }}, 'n/a') as state_cnty_fips_cd_{{ month }},
        coalesce(mdcr_status_cd_{{ month }}, 'n/a') as mdcr_status_cd_{{ month }},
        coalesce(mdcr_entlmt_buyin_ind_{{ month }}, 'n/a') as mdcr_entlmt_buyin_ind_{{ month }},
        coalesce(hmo_ind_{{ month }}, 'n/a') as hmo_ind_{{ month }},
        coalesce(ptc_cntrct_id_{{ month }}, 'n/a') as ptc_cntrct_id_{{ month }},
        coalesce(ptc_pbp_id_{{ month }}, 'n/a') as ptc_pbp_id_{{ month }},
        coalesce(ptc_plan_type_cd_{{ month }}, 'n/a') as ptc_plan_type_cd_{{ month }},
        coalesce(ptd_cntrct_id_{{ month }}, 'n/a') as ptd_cntrct_id_{{ month }},
        coalesce(ptd_pbp_id_{{ month }}, 'n/a') as ptd_pbp_id_{{ month }},
        coalesce(ptd_sgmt_id_{{ month }}, 'n/a') as ptd_sgmt_id_{{ month }},
        coalesce(rds_ind_{{ month }}, 'n/a') as rds_ind_{{ month }},
        coalesce(dual_stus_cd_{{ month }}, 'n/a') as dual_stus_cd_{{ month }},
        coalesce(cst_shr_grp_cd_{{ month }}, 'n/a') as cst_shr_grp_cd_{{ month }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM deduplicated_bene
WHERE rn = 1