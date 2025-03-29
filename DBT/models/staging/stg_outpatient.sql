{{
    config(
        materialized='view'
    )
}}

with outpatient_data as 
(
select *,
    row_number() over(partition by CLM_ID, CLM_LINE_NUM) as rn
  from {{ source('staging','outpatient') }}
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
    SAFE.PARSE_DATE('%d-%b-%Y', REV_CNTR_DT) AS rev_cntr_dt,
        


    -- info
    {{ dbt.safe_cast('CLM_PMT_AMT', api.Column.translate_type("numeric")) }}                                                               as clm_pmt_amt,
    {{ dbt.safe_cast('CLM_TOT_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                          as clm_tot_chrg_amt,
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
    {{ dbt.safe_cast('PRVDR_STATE_CD', api.Column.translate_type("string")) }}                                                               as prvdr_state_cd,
    {{ dbt.safe_cast('CLAIM_QUERY_CODE', api.Column.translate_type("string")) }}                                                             as claim_query_code,
    {{ dbt.safe_cast('REV_CNTR', api.Column.translate_type("string")) }}                                                                     as rev_cntr,
    {{ dbt.safe_cast('HCPCS_CD', api.Column.translate_type("string")) }}                                                                     as hcpcs_cd,

    {{ dbt.safe_cast('REV_CNTR_RATE_AMT', api.Column.translate_type("numeric")) }}                                                         as rev_cntr_rate_amt,
    {{ dbt.safe_cast('REV_CNTR_TOT_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                     as rev_cntr_tot_chrg_amt, 
    {{ dbt.safe_cast('REV_CNTR_NCVRD_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                   as rev_cntr_ncvrd_chrg_amt,



    --others
    {{ dbt.safe_cast('REV_CNTR_NDC_QTY', api.Column.translate_type("string")) }}                                                             as rev_cntr_ndc_qty,
    {{ dbt.safe_cast('REV_CNTR_NDC_QTY_QLFR_CD', api.Column.translate_type("string")) }}                                                     as rev_cntr_ndc_qty_qlfr_cd,
    {{ dbt.safe_cast('RNDRNG_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                            as rndrng_physn_upin,
    {{ dbt.safe_cast('RNDRNG_PHYSN_NPI', api.Column.translate_type("string")) }}                                                             as rndrng_physn_npi,
    {{ dbt.safe_cast('REV_CNTR_UNIT_CNT', api.Column.translate_type("string")) }}                                                            as rev_cntr_unit_cnt,
    {{ dbt.safe_cast('PRVDR_NUM', api.Column.translate_type("string")) }}                                                                    as prvdr_num,

   
    {{ dbt.safe_cast('OP_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                             as op_physn_upin,
    {{ dbt.safe_cast('OP_PHYSN_NPI', api.Column.translate_type("string")) }}                                                              as op_physn_npi,
    {{ dbt.safe_cast('OT_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                             as ot_physn_upin,
    {{ dbt.safe_cast('OT_PHYSN_NPI', api.Column.translate_type("string")) }}                                                              as ot_physn_npi,
    {{ dbt.safe_cast('CLM_MCO_PD_SW', api.Column.translate_type("string")) }}                                                                as clm_mco_pd_sw,
    {{ dbt.safe_cast('NCH_BENE_BLOOD_DDCTBL_LBLTY_AM', api.Column.translate_type("numeric")) }}                                            as nch_bene_blood_ddctbl_lblty_am,



    {{ dbt.safe_cast('ICD_PRCDR_CD1', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd1,
    {{ dbt.safe_cast('PRCDR_DT1', api.Column.translate_type("string")) }}                                                                    as prcdr_dt1,
    {{ dbt.safe_cast('ICD_PRCDR_CD2', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd2,
    {{ dbt.safe_cast('PRCDR_DT2', api.Column.translate_type("string")) }}                                                                    as prcdr_dt2,
    {{ dbt.safe_cast('ICD_PRCDR_CD3', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd3,
    {{ dbt.safe_cast('PRCDR_DT3', api.Column.translate_type("string")) }}                                                                    as prcdr_dt3,
    {{ dbt.safe_cast('ICD_PRCDR_CD4', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd4,
    {{ dbt.safe_cast('PRCDR_DT4', api.Column.translate_type("string")) }}                                                                    as prcdr_dt4,
    {{ dbt.safe_cast('ICD_PRCDR_CD5', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd5,
    {{ dbt.safe_cast('PRCDR_DT5', api.Column.translate_type("string")) }}                                                                    as prcdr_dt5,
    {{ dbt.safe_cast('ICD_PRCDR_CD6', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd6,
    {{ dbt.safe_cast('PRCDR_DT6', api.Column.translate_type("string")) }}                                                                    as prcdr_dt6,
    {{ dbt.safe_cast('ICD_PRCDR_CD7', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd7,
    {{ dbt.safe_cast('PRCDR_DT7', api.Column.translate_type("string")) }}                                                                    as prcdr_dt7,
    {{ dbt.safe_cast('ICD_PRCDR_CD8', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd8,
    {{ dbt.safe_cast('PRCDR_DT8', api.Column.translate_type("string")) }}                                                                    as prcdr_dt8,
    {{ dbt.safe_cast('ICD_PRCDR_CD9', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd9,
    {{ dbt.safe_cast('PRCDR_DT9', api.Column.translate_type("string")) }}                                                                    as prcdr_dt9,
    {{ dbt.safe_cast('ICD_PRCDR_CD10', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd10,
    {{ dbt.safe_cast('PRCDR_DT10', api.Column.translate_type("string")) }}                                                                    as prcdr_dt10,
    {{ dbt.safe_cast('ICD_PRCDR_CD11', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd11,
    {{ dbt.safe_cast('PRCDR_DT11', api.Column.translate_type("string")) }}                                                                    as prcdr_dt11,
    {{ dbt.safe_cast('ICD_PRCDR_CD12', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd12,
    {{ dbt.safe_cast('PRCDR_DT12', api.Column.translate_type("string")) }}                                                                    as prcdr_dt12,
    {{ dbt.safe_cast('ICD_PRCDR_CD13', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd13,
    {{ dbt.safe_cast('PRCDR_DT13', api.Column.translate_type("string")) }}                                                                    as prcdr_dt13,
    {{ dbt.safe_cast('ICD_PRCDR_CD14', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd14,
    {{ dbt.safe_cast('PRCDR_DT14', api.Column.translate_type("string")) }}                                                                    as prcdr_dt14,
    {{ dbt.safe_cast('ICD_PRCDR_CD15', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd15,
    {{ dbt.safe_cast('PRCDR_DT15', api.Column.translate_type("string")) }}                                                                    as prcdr_dt15,
    {{ dbt.safe_cast('ICD_PRCDR_CD16', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd16,
    {{ dbt.safe_cast('PRCDR_DT16', api.Column.translate_type("string")) }}                                                                    as prcdr_dt16,
    {{ dbt.safe_cast('ICD_PRCDR_CD17', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd17,
    {{ dbt.safe_cast('PRCDR_DT17', api.Column.translate_type("string")) }}                                                                    as prcdr_dt17,
    {{ dbt.safe_cast('ICD_PRCDR_CD18', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd18,
    {{ dbt.safe_cast('PRCDR_DT18', api.Column.translate_type("string")) }}                                                                    as prcdr_dt18,
    {{ dbt.safe_cast('ICD_PRCDR_CD19', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd19,
    {{ dbt.safe_cast('PRCDR_DT19', api.Column.translate_type("string")) }}                                                                    as prcdr_dt19,
    {{ dbt.safe_cast('ICD_PRCDR_CD20', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd20,
    {{ dbt.safe_cast('PRCDR_DT20', api.Column.translate_type("string")) }}                                                                    as prcdr_dt20,
    {{ dbt.safe_cast('ICD_PRCDR_CD21', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd21,
    {{ dbt.safe_cast('PRCDR_DT21', api.Column.translate_type("string")) }}                                                                    as prcdr_dt21,
    {{ dbt.safe_cast('ICD_PRCDR_CD22', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd22,
    {{ dbt.safe_cast('PRCDR_DT22', api.Column.translate_type("string")) }}                                                                    as prcdr_dt22,
    {{ dbt.safe_cast('ICD_PRCDR_CD23', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd23,
    {{ dbt.safe_cast('PRCDR_DT23', api.Column.translate_type("string")) }}                                                                    as prcdr_dt23,
    {{ dbt.safe_cast('ICD_PRCDR_CD24', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd24,
    {{ dbt.safe_cast('PRCDR_DT24', api.Column.translate_type("string")) }}                                                                    as prcdr_dt24,
    {{ dbt.safe_cast('ICD_PRCDR_CD25', api.Column.translate_type("string")) }}                                                                as icd_prcdr_cd25,
    {{ dbt.safe_cast('PRCDR_DT25', api.Column.translate_type("string")) }}                                                                    as prcdr_dt25,


    {{ dbt.safe_cast('NCH_PROFNL_CMPNT_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                  as nch_profnl_cmpnt_chrg_amt,
    {{ dbt.safe_cast('RSN_VISIT_CD1', api.Column.translate_type("string")) }}                                                                 as rsn_visit_cd1,
    {{ dbt.safe_cast('RSN_VISIT_CD2', api.Column.translate_type("string")) }}                                                                 as rsn_visit_cd2,
    {{ dbt.safe_cast('RSN_VISIT_CD3', api.Column.translate_type("string")) }}                                                                 as rsn_visit_cd3,
    {{ dbt.safe_cast('NCH_BENE_PTB_DDCTBL_AMT', api.Column.translate_type("numeric")) }}                                                    as nch_bene_ptb_ddctbl_amt,
    {{ dbt.safe_cast('NCH_BENE_PTB_COINSRNC_AMT', api.Column.translate_type("numeric")) }}                                                  as nch_bene_ptb_coinsrnc_amt,
    {{ dbt.safe_cast('CLM_OP_PRVDR_PMT_AMT', api.Column.translate_type("numeric")) }}                                                       as clm_op_prvdr_pmt_amt,
    {{ dbt.safe_cast('CLM_OP_BENE_PMT_AMT', api.Column.translate_type("numeric")) }}                                                        as clm_op_bene_pmt_amt,
    {{ dbt.safe_cast('REV_CNTR_1ST_ANSI_CD', api.Column.translate_type("string")) }}                                                          as rev_cntr_1st_ansi_cd,


    {{ dbt.safe_cast('REV_CNTR_2ND_ANSI_CD', api.Column.translate_type("string")) }}                                                          as rev_cntr_2nd_ansi_cd,
    {{ dbt.safe_cast('REV_CNTR_3RD_ANSI_CD', api.Column.translate_type("string")) }}                                                          as rev_cntr_3rd_ansi_cd,
    {{ dbt.safe_cast('REV_CNTR_4TH_ANSI_CD', api.Column.translate_type("string")) }}                                                          as rev_cntr_4th_ansi_cd,
    {{ dbt.safe_cast('REV_CNTR_APC_HIPPS_CD', api.Column.translate_type("string")) }}                                                         as rev_cntr_apc_hipps_cd,
    {{ dbt.safe_cast('HCPCS_1ST_MDFR_CD', api.Column.translate_type("string")) }}                                                             as hcpcs_1st_mdfr_cd,
    {{ dbt.safe_cast('HCPCS_2ND_MDFR_CD', api.Column.translate_type("string")) }}                                                             as hcpcs_2nd_mdfr_cd,
    {{ dbt.safe_cast('REV_CNTR_PMT_MTHD_IND_CD', api.Column.translate_type("string")) }}                                                      as rev_cntr_pmt_mthd_ind_cd,
    {{ dbt.safe_cast('REV_CNTR_DSCNT_IND_CD', api.Column.translate_type("string")) }}                                                         as rev_cntr_dscnt_ind_cd,
    {{ dbt.safe_cast('REV_CNTR_PACKG_IND_CD', api.Column.translate_type("string")) }}                                                         as rev_cntr_packg_ind_cd,
    {{ dbt.safe_cast('REV_CNTR_OTAF_PMT_CD', api.Column.translate_type("string")) }}                                                          as rev_cntr_otaf_pmt_cd,



    {{ dbt.safe_cast('REV_CNTR_IDE_NDC_UPC_NUM', api.Column.translate_type("string")) }}                                                      as rev_cntr_ide_ndc_upc_num,
    {{ dbt.safe_cast('REV_CNTR_BLOOD_DDCTBL_AMT', api.Column.translate_type("numeric")) }}                                                  as rev_cntr_blood_ddctbl_amt,
    {{ dbt.safe_cast('REV_CNTR_CASH_DDCTBL_AMT', api.Column.translate_type("numeric")) }}                                                   as rev_cntr_cash_ddctbl_amt,
    {{ dbt.safe_cast('REV_CNTR_COINSRNC_WGE_ADJSTD_C', api.Column.translate_type("numeric")) }}                                             as rev_cntr_coinsrnc_wge_adjstd_c,
    {{ dbt.safe_cast('REV_CNTR_RDCD_COINSRNC_AMT', api.Column.translate_type("numeric")) }}                                                 as rev_cntr_rdcd_coinsrnc_amt,
    {{ dbt.safe_cast('REV_CNTR_1ST_MSP_PD_AMT', api.Column.translate_type("numeric")) }}                                                    as rev_cntr_1st_msp_pd_amt,
    {{ dbt.safe_cast('REV_CNTR_2ND_MSP_PD_AMT', api.Column.translate_type("numeric")) }}                                                    as rev_cntr_2nd_msp_pd_amt,
    {{ dbt.safe_cast('REV_CNTR_PRVDR_PMT_AMT', api.Column.translate_type("numeric")) }}                                                     as rev_cntr_prvdr_pmt_amt,
    {{ dbt.safe_cast('REV_CNTR_BENE_PMT_AMT', api.Column.translate_type("numeric")) }}                                                      as rev_cntr_bene_pmt_amt,
    {{ dbt.safe_cast('REV_CNTR_PTNT_RSPNSBLTY_PMT', api.Column.translate_type("numeric")) }}                                                as rev_cntr_ptnt_rspnsblty_pmt,


    {{ dbt.safe_cast('REV_CNTR_PMT_AMT_AMT', api.Column.translate_type("numeric")) }}                                                       as rev_cntr_pmt_amt_amt,
    {{ dbt.safe_cast('REV_CNTR_STUS_IND_CD', api.Column.translate_type("string")) }}                                                          as rev_cntr_stus_ind_cd   

from outpatient_data
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}