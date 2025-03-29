{{
    config(
        materialized='view'
    )
}}

with bene_data_2022 as 
(
  select *,
    row_number() over(partition by BENE_ID, BENE_BIRTH_DT) as rn
  from {{ source('staging','beneficiary_2022') }}
  where BENE_ID is not null 
)
select
    -- identifiers
    {{ dbt.safe_cast('REPLACE(CAST(BENE_ID AS STRING), \'-\', \'\')', api.Column.translate_type("bigint")) }}                                 as bene_id,
    {{ dbt.safe_cast('STATE_CODE', api.Column.translate_type("string")) }}                                                                    as state_code,
    {{ dbt.safe_cast('SEX_IDENT_CD', api.Column.translate_type("string")) }}                                                                  as sex_cd,
    CASE
      WHEN {{ dbt.safe_cast('SEX_IDENT_CD', api.Column.translate_type("string")) }} = '0' THEN 'Unknown'
      WHEN {{ dbt.safe_cast('SEX_IDENT_CD', api.Column.translate_type("string")) }} = '1' THEN 'Male'
      WHEN {{ dbt.safe_cast('SEX_IDENT_CD', api.Column.translate_type("string")) }} = '2' THEN 'Female'
    ELSE
      'Empty'
    END                                                                                                                                       as gender,
    {{ dbt.safe_cast('BENE_RACE_CD', api.Column.translate_type("string")) }}                                                                  as race_cd,
    {{ race_description('BENE_RACE_CD') }}                                                                                                    as race,
    {{ dbt.safe_cast('AGE_AT_END_REF_YR', api.Column.translate_type("integer")) }}                                                          as age_at_end_ref_yr,
    {{ dbt.safe_cast('RTI_RACE_CD', api.Column.translate_type("string")) }}                                                                   as rti_race_cd,  
    

    -- dates

    SAFE.PARSE_DATE('%d-%b-%Y', BENE_BIRTH_DT) AS bene_birth_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', BENE_DEATH_DT) AS bene_death_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', COVSTART) AS covstart,

    {{ dbt.safe_cast('BENE_ENROLLMT_REF_YR', api.Column.translate_type("integer")) }}                                                         as enrollmnt_year,
    coalesce({{ dbt.safe_cast('VALID_DEATH_DT_SW', api.Column.translate_type("string")) }},'N')                                               as valid_death_dt_sw,

    
    -- info
    {{ dbt.safe_cast('ENTLMT_RSN_ORIG', api.Column.translate_type("string")) }}                                                               as entlmt_rsn_orig,
    {{ dbt.safe_cast('ENTLMT_RSN_CURR', api.Column.translate_type("string")) }}                                                               as entlmt_rsn_curr,

    --others
    {{ dbt.safe_cast('COUNTY_CD', api.Column.translate_type("string")) }}                                                                     as county_cd,
    {{ dbt.safe_cast('ZIP_CD', api.Column.translate_type("string")) }}                                                                        as zip_cd,
    {{ dbt.safe_cast('ESRD_IND', api.Column.translate_type("string")) }}                                                                      as esrd_ind,
    {{ dbt.safe_cast('BENE_PTA_TRMNTN_CD', api.Column.translate_type("integer")) }}                                                         as bene_pta_trmntn_cd,
    {{ dbt.safe_cast('BENE_PTB_TRMNTN_CD', api.Column.translate_type("integer")) }}                                                         as bene_ptb_trmntn_cd,
    {{ dbt.safe_cast('BENE_HI_CVRAGE_TOT_MONS', api.Column.translate_type("integer")) }}                                                    as bene_hi_cvrage_tot_mons,
    {{ dbt.safe_cast('BENE_SMI_CVRAGE_TOT_MONS', api.Column.translate_type("integer")) }}                                                   as bene_smi_cvrage_tot_mons,
    {{ dbt.safe_cast('BENE_STATE_BUYIN_TOT_MONS', api.Column.translate_type("integer")) }}                                                  as bene_state_buyin_tot_mons,
    {{ dbt.safe_cast('BENE_HMO_CVRAGE_TOT_MONS', api.Column.translate_type("integer")) }}                                                   as bene_hmo_cvrage_tot_mons,
    {{ dbt.safe_cast('RDS_CVRG_MONS', api.Column.translate_type("integer")) }}                                                              as rds_cvrg_mons,
    {{ dbt.safe_cast('ENRL_SRC', api.Column.translate_type("string")) }}                                                                      as enrl_src,
    {{ dbt.safe_cast('SAMPLE_GROUP', api.Column.translate_type("string")) }}                                                                  as sample_grp,
    {{ dbt.safe_cast('ENHANCED_FIVE_PERCENT_FLAG', api.Column.translate_type("string")) }}                                                    as enhanced_five_percent_flag,
    {{ dbt.safe_cast('CRNT_BIC_CD', api.Column.translate_type("string")) }}                                                                   as crnt_bic_cd,
    {{ dbt.safe_cast('DUAL_ELGBL_MONS', api.Column.translate_type("integer")) }}                                                            as dual_elgbl_mons,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_01', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_jan,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_02', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_feb,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_03', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_mar,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_04', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_apr,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_05', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_may,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_06', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_jun,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_07', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_jul,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_08', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_aug,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_09', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_sep,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_10', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_oct,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_11', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_nov,
    {{ dbt.safe_cast('STATE_CNTY_FIPS_CD_12', api.Column.translate_type("string")) }}                                                         as state_cnty_fips_cd_dec,

    {{ dbt.safe_cast('MDCR_STATUS_CODE_01', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_jan,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_02', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_feb,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_03', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_mar,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_04', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_apr,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_05', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_may,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_06', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_jun,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_07', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_jul,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_08', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_aug,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_09', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_sep,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_10', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_oct,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_11', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_nov,
    {{ dbt.safe_cast('MDCR_STATUS_CODE_12', api.Column.translate_type("string")) }}                                                           as mdcr_status_cd_dec, 

    {{ dbt.safe_cast('PTD_PLAN_CVRG_MONS', api.Column.translate_type("integer")) }}                                                         as ptd_plan_cvrg_mons,

    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_01', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_jan,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_02', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_feb,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_03', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_mar,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_04', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_apr,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_05', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_may,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_06', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_jun,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_07', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_jul,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_08', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_aug,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_09', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_sep,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_10', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_oct,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_11', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_nov,
    {{ dbt.safe_cast('MDCR_ENTLMT_BUYIN_IND_12', api.Column.translate_type("string")) }}                                                      as mdcr_entlmt_buyin_ind_dec, 

    {{ dbt.safe_cast('HMO_IND_01', api.Column.translate_type("string")) }}                                                                    as hmo_ind_jan,
    {{ dbt.safe_cast('HMO_IND_02', api.Column.translate_type("string")) }}                                                                    as hmo_ind_feb,
    {{ dbt.safe_cast('HMO_IND_03', api.Column.translate_type("string")) }}                                                                    as hmo_ind_mar,
    {{ dbt.safe_cast('HMO_IND_04', api.Column.translate_type("string")) }}                                                                    as hmo_ind_apr,
    {{ dbt.safe_cast('HMO_IND_05', api.Column.translate_type("string")) }}                                                                    as hmo_ind_may, 
    {{ dbt.safe_cast('HMO_IND_06', api.Column.translate_type("string")) }}                                                                    as hmo_ind_jun, 
    {{ dbt.safe_cast('HMO_IND_07', api.Column.translate_type("string")) }}                                                                    as hmo_ind_jul,
    {{ dbt.safe_cast('HMO_IND_08', api.Column.translate_type("string")) }}                                                                    as hmo_ind_aug, 
    {{ dbt.safe_cast('HMO_IND_09', api.Column.translate_type("string")) }}                                                                    as hmo_ind_sep, 
    {{ dbt.safe_cast('HMO_IND_10', api.Column.translate_type("string")) }}                                                                    as hmo_ind_oct, 
    {{ dbt.safe_cast('HMO_IND_11', api.Column.translate_type("string")) }}                                                                    as hmo_ind_nov, 
    {{ dbt.safe_cast('HMO_IND_12', api.Column.translate_type("string")) }}                                                                    as hmo_ind_dec,

    {{ dbt.safe_cast('PTC_CNTRCT_ID_01', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_jan,
    {{ dbt.safe_cast('PTC_CNTRCT_ID_02', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_feb,
    {{ dbt.safe_cast('PTC_CNTRCT_ID_03', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_mar,
    {{ dbt.safe_cast('PTC_CNTRCT_ID_04', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_apr,
    {{ dbt.safe_cast('PTC_CNTRCT_ID_05', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_may, 
    {{ dbt.safe_cast('PTC_CNTRCT_ID_06', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_jun, 
    {{ dbt.safe_cast('PTC_CNTRCT_ID_07', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_jul,
    {{ dbt.safe_cast('PTC_CNTRCT_ID_08', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_aug, 
    {{ dbt.safe_cast('PTC_CNTRCT_ID_09', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_sep, 
    {{ dbt.safe_cast('PTC_CNTRCT_ID_10', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_oct, 
    {{ dbt.safe_cast('PTC_CNTRCT_ID_11', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_nov, 
    {{ dbt.safe_cast('PTC_CNTRCT_ID_12', api.Column.translate_type("string")) }}                                                              as ptc_cntrct_id_dec,

    {{ dbt.safe_cast('PTC_PBP_ID_01', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_jan,
    {{ dbt.safe_cast('PTC_PBP_ID_02', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_feb,
    {{ dbt.safe_cast('PTC_PBP_ID_03', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_mar,
    {{ dbt.safe_cast('PTC_PBP_ID_04', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_apr,
    {{ dbt.safe_cast('PTC_PBP_ID_05', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_may, 
    {{ dbt.safe_cast('PTC_PBP_ID_06', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_jun, 
    {{ dbt.safe_cast('PTC_PBP_ID_07', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_jul,
    {{ dbt.safe_cast('PTC_PBP_ID_08', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_aug, 
    {{ dbt.safe_cast('PTC_PBP_ID_09', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_sep, 
    {{ dbt.safe_cast('PTC_PBP_ID_10', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_oct, 
    {{ dbt.safe_cast('PTC_PBP_ID_11', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_nov, 
    {{ dbt.safe_cast('PTC_PBP_ID_12', api.Column.translate_type("string")) }}                                                                 as ptc_pbp_id_dec,

    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_01', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_jan,
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_02', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_feb,
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_03', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_mar,
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_04', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_apr,
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_05', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_may, 
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_06', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_jun, 
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_07', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_jul,
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_08', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_aug, 
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_09', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_sep, 
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_10', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_oct, 
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_11', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_nov, 
    {{ dbt.safe_cast('PTC_PLAN_TYPE_CD_12', api.Column.translate_type("string")) }}                                                           as ptc_plan_type_cd_dec,    

    {{ dbt.safe_cast('PTD_CNTRCT_ID_01', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_jan,
    {{ dbt.safe_cast('PTD_CNTRCT_ID_02', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_feb,
    {{ dbt.safe_cast('PTD_CNTRCT_ID_03', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_mar,
    {{ dbt.safe_cast('PTD_CNTRCT_ID_04', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_apr,
    {{ dbt.safe_cast('PTD_CNTRCT_ID_05', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_may, 
    {{ dbt.safe_cast('PTD_CNTRCT_ID_06', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_jun, 
    {{ dbt.safe_cast('PTD_CNTRCT_ID_07', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_jul,
    {{ dbt.safe_cast('PTD_CNTRCT_ID_08', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_aug, 
    {{ dbt.safe_cast('PTD_CNTRCT_ID_09', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_sep, 
    {{ dbt.safe_cast('PTD_CNTRCT_ID_10', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_oct, 
    {{ dbt.safe_cast('PTD_CNTRCT_ID_11', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_nov, 
    {{ dbt.safe_cast('PTD_CNTRCT_ID_12', api.Column.translate_type("string")) }}                                                              as ptd_cntrct_id_dec,

    {{ dbt.safe_cast('PTD_PBP_ID_01', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_jan,
    {{ dbt.safe_cast('PTD_PBP_ID_02', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_feb,
    {{ dbt.safe_cast('PTD_PBP_ID_03', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_mar,
    {{ dbt.safe_cast('PTD_PBP_ID_04', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_apr,
    {{ dbt.safe_cast('PTD_PBP_ID_05', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_may, 
    {{ dbt.safe_cast('PTD_PBP_ID_06', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_jun, 
    {{ dbt.safe_cast('PTD_PBP_ID_07', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_jul,
    {{ dbt.safe_cast('PTD_PBP_ID_08', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_aug, 
    {{ dbt.safe_cast('PTD_PBP_ID_09', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_sep, 
    {{ dbt.safe_cast('PTD_PBP_ID_10', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_oct, 
    {{ dbt.safe_cast('PTD_PBP_ID_11', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_nov, 
    {{ dbt.safe_cast('PTD_PBP_ID_12', api.Column.translate_type("string")) }}                                                                 as ptd_pbp_id_dec,

    {{ dbt.safe_cast('PTD_SGMT_ID_01', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_jan,
    {{ dbt.safe_cast('PTD_SGMT_ID_02', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_feb,
    {{ dbt.safe_cast('PTD_SGMT_ID_03', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_mar,
    {{ dbt.safe_cast('PTD_SGMT_ID_04', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_apr,
    {{ dbt.safe_cast('PTD_SGMT_ID_05', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_may, 
    {{ dbt.safe_cast('PTD_SGMT_ID_06', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_jun, 
    {{ dbt.safe_cast('PTD_SGMT_ID_07', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_jul,
    {{ dbt.safe_cast('PTD_SGMT_ID_08', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_aug, 
    {{ dbt.safe_cast('PTD_SGMT_ID_09', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_sep, 
    {{ dbt.safe_cast('PTD_SGMT_ID_10', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_oct, 
    {{ dbt.safe_cast('PTD_SGMT_ID_11', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_nov, 
    {{ dbt.safe_cast('PTD_SGMT_ID_12', api.Column.translate_type("string")) }}                                                                as ptd_sgmt_id_dec,    


    {{ dbt.safe_cast('RDS_IND_01', api.Column.translate_type("string")) }}                                                                    as rds_ind_jan,
    {{ dbt.safe_cast('RDS_IND_02', api.Column.translate_type("string")) }}                                                                    as rds_ind_feb,
    {{ dbt.safe_cast('RDS_IND_03', api.Column.translate_type("string")) }}                                                                    as rds_ind_mar,
    {{ dbt.safe_cast('RDS_IND_04', api.Column.translate_type("string")) }}                                                                    as rds_ind_apr,
    {{ dbt.safe_cast('RDS_IND_05', api.Column.translate_type("string")) }}                                                                    as rds_ind_may, 
    {{ dbt.safe_cast('RDS_IND_06', api.Column.translate_type("string")) }}                                                                    as rds_ind_jun, 
    {{ dbt.safe_cast('RDS_IND_07', api.Column.translate_type("string")) }}                                                                    as rds_ind_jul,
    {{ dbt.safe_cast('RDS_IND_08', api.Column.translate_type("string")) }}                                                                    as rds_ind_aug, 
    {{ dbt.safe_cast('RDS_IND_09', api.Column.translate_type("string")) }}                                                                    as rds_ind_sep, 
    {{ dbt.safe_cast('RDS_IND_10', api.Column.translate_type("string")) }}                                                                    as rds_ind_oct, 
    {{ dbt.safe_cast('RDS_IND_11', api.Column.translate_type("string")) }}                                                                    as rds_ind_nov, 
    {{ dbt.safe_cast('RDS_IND_12', api.Column.translate_type("string")) }}                                                                    as rds_ind_dec,

    {{ dbt.safe_cast('DUAL_STUS_CD_01', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_jan,
    {{ dbt.safe_cast('DUAL_STUS_CD_02', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_feb,
    {{ dbt.safe_cast('DUAL_STUS_CD_03', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_mar,
    {{ dbt.safe_cast('DUAL_STUS_CD_04', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_apr,
    {{ dbt.safe_cast('DUAL_STUS_CD_05', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_may, 
    {{ dbt.safe_cast('DUAL_STUS_CD_06', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_jun, 
    {{ dbt.safe_cast('DUAL_STUS_CD_07', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_jul,
    {{ dbt.safe_cast('DUAL_STUS_CD_08', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_aug, 
    {{ dbt.safe_cast('DUAL_STUS_CD_09', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_sep, 
    {{ dbt.safe_cast('DUAL_STUS_CD_10', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_oct, 
    {{ dbt.safe_cast('DUAL_STUS_CD_11', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_nov, 
    {{ dbt.safe_cast('DUAL_STUS_CD_12', api.Column.translate_type("string")) }}                                                               as dual_stus_cd_dec,

    {{ dbt.safe_cast('CST_SHR_GRP_CD_01', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_jan,
    {{ dbt.safe_cast('CST_SHR_GRP_CD_02', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_feb,
    {{ dbt.safe_cast('CST_SHR_GRP_CD_03', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_mar,
    {{ dbt.safe_cast('CST_SHR_GRP_CD_04', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_apr,
    {{ dbt.safe_cast('CST_SHR_GRP_CD_05', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_may, 
    {{ dbt.safe_cast('CST_SHR_GRP_CD_06', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_jun, 
    {{ dbt.safe_cast('CST_SHR_GRP_CD_07', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_jul,
    {{ dbt.safe_cast('CST_SHR_GRP_CD_08', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_aug, 
    {{ dbt.safe_cast('CST_SHR_GRP_CD_09', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_sep, 
    {{ dbt.safe_cast('CST_SHR_GRP_CD_10', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_oct, 
    {{ dbt.safe_cast('CST_SHR_GRP_CD_11', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_nov,
    {{ dbt.safe_cast('CST_SHR_GRP_CD_12', api.Column.translate_type("string")) }}                                                             as cst_shr_grp_cd_dec
from bene_data_2022
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}