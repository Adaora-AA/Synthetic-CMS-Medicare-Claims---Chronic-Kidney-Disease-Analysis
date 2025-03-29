{{
    config(
        materialized='view'
    )
}}

with hospice_data as 
(
    select *,
    row_number() over(partition by BENE_ID,CLM_ID, CLM_LINE_NUM) as rn
  from {{ source('staging','hospice') }}
  where BENE_ID is not null  
)
select
    -- identifiers
    {{ dbt.safe_cast('REPLACE(CAST(BENE_ID AS STRING), \'-\', \'\')', api.Column.translate_type("bigint")) }}                                as bene_id,
    {{ dbt.safe_cast('REPLACE(CAST(CLM_ID AS STRING), \'-\', \'\')', api.Column.translate_type("bigint")) }}                                 as clm_id,
    {{ dbt.safe_cast('NCH_NEAR_LINE_REC_IDENT_CD', api.Column.translate_type("string")) }}                                                   as nch_near_line_rec_ident_cd,
    {{ dbt.safe_cast('NCH_CLM_TYPE_CD', api.Column.translate_type("string")) }}                                                              as nch_clm_type_cd, 
    

    -- dates
    SAFE.PARSE_DATE('%d-%b-%Y', CLM_FROM_DT) AS clm_from_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', CLM_THRU_DT) AS clm_thru_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', NCH_WKLY_PROC_DT) AS nch_wkly_proc_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', FI_CLM_PROC_DT) AS fi_clm_proc_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', CLM_HOSPC_START_DT_ID) AS clm_hospc_start_dt_id,
    SAFE.PARSE_DATE('%d-%b-%Y', REV_CNTR_DT) AS rev_cntr_dt, 
    SAFE.PARSE_DATE('%d-%b-%Y', NCH_BENE_DSCHRG_DT) AS nch_bene_dschrg_dt, 

    
    -- info
    {{ dbt.safe_cast('CLM_PMT_AMT', api.Column.translate_type("numeric")) }}                                                               as clm_pmt_amt,
    {{ dbt.safe_cast('CLM_TOT_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                          as clm_tot_chrg_amt,
    {{ dbt.safe_cast('REV_CNTR_PRVDR_PMT_AMT', api.Column.translate_type("numeric")) }}                                                    as rev_cntr_prvdr_pmt_amt,
    {{ dbt.safe_cast('REV_CNTR_BENE_PMT_AMT', api.Column.translate_type("numeric")) }}                                                     as rev_cntr_bene_pmt_amt,
    {{ dbt.safe_cast('NCH_PRMRY_PYR_CLM_PD_AMT', api.Column.translate_type("numeric")) }}                                                  as nch_prmry_pyr_clm_pd_amt,
    {{ dbt.safe_cast('CLM_FAC_TYPE_CD', api.Column.translate_type("numeric")) }}                                                           as clm_fac_type_cd,
    {{ dbt.safe_cast('CLM_SRVC_CLSFCTN_TYPE_CD', api.Column.translate_type("numeric")) }}                                                  as clm_srvc_clsfctn_type_cd,
    {{ dbt.safe_cast('CLM_FREQ_CD', api.Column.translate_type("string")) }}                                                                  as clm_freq_cd,

    {{ dbt.safe_cast('PRNCPAL_DGNS_CD', api.Column.translate_type("string")) }}                                                              as prncpal_dgns_cd,
    {{ dbt.safe_cast('FI_NUM', api.Column.translate_type("string")) }}                                                                       as fi_num,
    {{ dbt.safe_cast('ICD_DGNS_CD1', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd1,
    {{ dbt.safe_cast('CLM_MDCR_NON_PMT_RSN_CD', api.Column.translate_type("string")) }}                                                      as clm_mdcr_non_pmt_rsn_cd,
    {{ dbt.safe_cast('ICD_DGNS_CD2', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd2,
    {{ dbt.safe_cast('NCH_PRMRY_PYR_CD', api.Column.translate_type("string")) }}                                                             as nch_prmry_pyr_cd,
    {{ dbt.safe_cast('ICD_DGNS_CD3', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd3,
    {{ dbt.safe_cast('PTNT_DSCHRG_STUS_CD', api.Column.translate_type("string")) }}                                                          as ptnt_dschrg_stus_cd,
    {{ dbt.safe_cast('ICD_DGNS_CD4', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd4,
    {{ dbt.safe_cast('ICD_DGNS_CD5', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd5,
    {{ dbt.safe_cast('ICD_DGNS_CD6', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd6,
    {{ dbt.safe_cast('ICD_DGNS_CD7', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd7,
    {{ dbt.safe_cast('ICD_DGNS_CD8', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd8,
    {{ dbt.safe_cast('ICD_DGNS_CD9', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd9,
    {{ dbt.safe_cast('ICD_DGNS_CD10', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd10,
    {{ dbt.safe_cast('ICD_DGNS_CD11', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd11,
    {{ dbt.safe_cast('ICD_DGNS_CD12', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd12,
    {{ dbt.safe_cast('ICD_DGNS_CD13', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd13,
    {{ dbt.safe_cast('ICD_DGNS_CD14', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd14,
    {{ dbt.safe_cast('ICD_DGNS_CD15', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd15,
    {{ dbt.safe_cast('ICD_DGNS_CD16', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd16,
    {{ dbt.safe_cast('ICD_DGNS_CD17', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd17,
    {{ dbt.safe_cast('ICD_DGNS_CD18', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd18,
    {{ dbt.safe_cast('ICD_DGNS_CD19', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd19,
    {{ dbt.safe_cast('ICD_DGNS_CD20', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd20,
    {{ dbt.safe_cast('ICD_DGNS_CD21', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd21,
    {{ dbt.safe_cast('ICD_DGNS_CD22', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd22,
    {{ dbt.safe_cast('ICD_DGNS_CD23', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd23,
    {{ dbt.safe_cast('ICD_DGNS_CD24', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd24,
    {{ dbt.safe_cast('ICD_DGNS_CD25', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd25,

    {{ dbt.safe_cast('FST_DGNS_E_CD', api.Column.translate_type("string")) }}                                                                as fst_dgns_e_cd,
    {{ dbt.safe_cast('ICD_DGNS_E_CD1', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd1,
    {{ dbt.safe_cast('ICD_DGNS_E_CD2', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd2,
    {{ dbt.safe_cast('ICD_DGNS_E_CD3', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd3,
    {{ dbt.safe_cast('ICD_DGNS_E_CD4', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd4,
    {{ dbt.safe_cast('ICD_DGNS_E_CD5', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd5,
    {{ dbt.safe_cast('ICD_DGNS_E_CD6', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd6,
    {{ dbt.safe_cast('ICD_DGNS_E_CD7', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd7,
    {{ dbt.safe_cast('ICD_DGNS_E_CD8', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd8,
    {{ dbt.safe_cast('ICD_DGNS_E_CD9', api.Column.translate_type("string")) }}                                                               as icd_dgns_e_cd9,
    {{ dbt.safe_cast('ICD_DGNS_E_CD10', api.Column.translate_type("string")) }}                                                              as icd_dgns_e_cd10,
    {{ dbt.safe_cast('ICD_DGNS_E_CD11', api.Column.translate_type("string")) }}                                                              as icd_dgns_e_cd11,
    {{ dbt.safe_cast('ICD_DGNS_E_CD12', api.Column.translate_type("string")) }}                                                              as icd_dgns_e_cd12,


    {{ dbt.safe_cast('CLM_LINE_NUM', api.Column.translate_type("numeric")) }}                                                              as clm_line_num,
    {{ dbt.safe_cast('AT_PHYSN_NPI', api.Column.translate_type("string")) }}                                                                 as at_physn_npi,
    {{ dbt.safe_cast('AT_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                                as at_physn_upin,
    {{ dbt.safe_cast('ORG_NPI_NUM', api.Column.translate_type("string")) }}                                                                  as org_npi_num,
    {{ dbt.safe_cast('NCH_PTNT_STATUS_IND_CD', api.Column.translate_type("string")) }}                                                       as nch_ptnt_status_ind_cd,
    {{ dbt.safe_cast('CLM_UTLZTN_DAY_CNT', api.Column.translate_type("string")) }}                                                           as clm_utlztn_day_cnt,
    {{ dbt.safe_cast('PRVDR_STATE_CD', api.Column.translate_type("string")) }}                                                               as prvdr_state_cd,
    {{ dbt.safe_cast('BENE_HOSPC_PRD_CNT', api.Column.translate_type("string")) }}                                                           as bene_hospc_prd_cnt,
    {{ dbt.safe_cast('CLAIM_QUERY_CODE', api.Column.translate_type("string")) }}                                                             as claim_query_code,
    {{ dbt.safe_cast('REV_CNTR', api.Column.translate_type("string")) }}                                                                     as rev_cntr,
    {{ dbt.safe_cast('HCPCS_CD', api.Column.translate_type("string")) }}                                                                     as hcpcs_cd,
    {{ dbt.safe_cast('HCPCS_1ST_MDFR_CD', api.Column.translate_type("string")) }}                                                            as hcpcs_1st_mdfr_cd,
    {{ dbt.safe_cast('HCPCS_2ND_MDFR_CD', api.Column.translate_type("string")) }}                                                            as hcpcs_2nd_mdfr_cd,

    {{ dbt.safe_cast('REV_CNTR_RATE_AMT', api.Column.translate_type("numeric")) }}                                                         as rev_cntr_rate_amt,
    {{ dbt.safe_cast('REV_CNTR_PMT_AMT_AMT', api.Column.translate_type("numeric")) }}                                                      as rev_cntr_pmt_amt_amt,
    {{ dbt.safe_cast('REV_CNTR_TOT_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                     as rev_cntr_tot_chrg_amt, 
    {{ dbt.safe_cast('REV_CNTR_NCVRD_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                   as rev_cntr_ncvrd_chrg_amt,
    


    --others

    
    {{ dbt.safe_cast('REV_CNTR_DDCTBL_COINSRNC_CD', api.Column.translate_type("string")) }}                                                  as rev_cntr_ddctbl_coinsrnc_cd,
    {{ dbt.safe_cast('REV_CNTR_NDC_QTY', api.Column.translate_type("string")) }}                                                             as rev_cntr_ndc_qty,
    {{ dbt.safe_cast('REV_CNTR_NDC_QTY_QLFR_CD', api.Column.translate_type("string")) }}                                                     as rev_cntr_ndc_qty_qlfr_cd,
    {{ dbt.safe_cast('RNDRNG_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                            as rndrng_physn_upin,
    {{ dbt.safe_cast('RNDRNG_PHYSN_NPI', api.Column.translate_type("string")) }}                                                             as rndrng_physn_npi,
    {{ dbt.safe_cast('REV_CNTR_UNIT_CNT', api.Column.translate_type("string")) }}                                                            as rev_cntr_unit_cnt,
    {{ dbt.safe_cast('PRVDR_NUM', api.Column.translate_type("string")) }}                                                                    as prvdr_num

from hospice_data
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}