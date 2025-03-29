{{
    config(
        materialized='view'
    )
}}

with inpatient_data as 
(
  select *,
    row_number() over(partition by BENE_ID,CLM_ID, CLM_LINE_NUM) as rn
  from {{ source('staging','inpatient') }}
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
    SAFE.PARSE_DATE('%d-%b-%Y', CLM_ADMSN_DT) AS clm_admsn_dt,

    SAFE.PARSE_DATE('%d-%b-%Y', NCH_BENE_DSCHRG_DT) AS nch_bene_dschrg_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', NCH_VRFD_NCVRD_STAY_FROM_DT) AS nch_vrfd_ncvrd_stay_from_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', NCH_VRFD_NCVRD_STAY_THRU_DT) AS nch_vrfd_ncvrd_stay_thru_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', NCH_ACTV_OR_CVRD_LVL_CARE_THRU) AS nch_actv_or_cvrd_lvl_care_thru,
    SAFE.PARSE_DATE('%d-%b-%Y', NCH_BENE_MDCR_BNFTS_EXHTD_DT_I) AS nch_bene_mdcr_bnfts_exhtd_dt_i,
    

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


    --others
    {{ dbt.safe_cast('PRVDR_NUM', api.Column.translate_type("string")) }}                                                                    as prvdr_num,
    {{ dbt.safe_cast('OP_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                                as op_physn_upin,
    {{ dbt.safe_cast('OP_PHYSN_NPI', api.Column.translate_type("string")) }}                                                                 as op_physn_npi,
    {{ dbt.safe_cast('OT_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                                as ot_physn_upin,
    {{ dbt.safe_cast('OT_PHYSN_NPI', api.Column.translate_type("string")) }}                                                                 as ot_physn_npi,
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
    {{ dbt.safe_cast('ICD_PRCDR_CD10', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd10,
    {{ dbt.safe_cast('PRCDR_DT10', api.Column.translate_type("string")) }}                                                                   as prcdr_dt10,
    {{ dbt.safe_cast('ICD_PRCDR_CD11', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd11,
    {{ dbt.safe_cast('PRCDR_DT11', api.Column.translate_type("string")) }}                                                                   as prcdr_dt11,
    {{ dbt.safe_cast('ICD_PRCDR_CD12', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd12,
    {{ dbt.safe_cast('PRCDR_DT12', api.Column.translate_type("string")) }}                                                                   as prcdr_dt12,
    {{ dbt.safe_cast('ICD_PRCDR_CD13', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd13,
    {{ dbt.safe_cast('PRCDR_DT13', api.Column.translate_type("string")) }}                                                                   as prcdr_dt13,
    {{ dbt.safe_cast('ICD_PRCDR_CD14', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd14,
    {{ dbt.safe_cast('PRCDR_DT14', api.Column.translate_type("string")) }}                                                                   as prcdr_dt14,
    {{ dbt.safe_cast('ICD_PRCDR_CD15', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd15,
    {{ dbt.safe_cast('PRCDR_DT15', api.Column.translate_type("string")) }}                                                                   as prcdr_dt15,
    {{ dbt.safe_cast('ICD_PRCDR_CD16', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd16,
    {{ dbt.safe_cast('PRCDR_DT16', api.Column.translate_type("string")) }}                                                                   as prcdr_dt16,
    {{ dbt.safe_cast('ICD_PRCDR_CD17', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd17,
    {{ dbt.safe_cast('PRCDR_DT17', api.Column.translate_type("string")) }}                                                                   as prcdr_dt17,
    {{ dbt.safe_cast('ICD_PRCDR_CD18', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd18,
    {{ dbt.safe_cast('PRCDR_DT18', api.Column.translate_type("string")) }}                                                                   as prcdr_dt18,
    {{ dbt.safe_cast('ICD_PRCDR_CD19', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd19,
    {{ dbt.safe_cast('PRCDR_DT19', api.Column.translate_type("string")) }}                                                                   as prcdr_dt19,
    {{ dbt.safe_cast('ICD_PRCDR_CD20', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd20,
    {{ dbt.safe_cast('PRCDR_DT20', api.Column.translate_type("string")) }}                                                                   as prcdr_dt20,
    {{ dbt.safe_cast('ICD_PRCDR_CD21', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd21,
    {{ dbt.safe_cast('PRCDR_DT21', api.Column.translate_type("string")) }}                                                                   as prcdr_dt21,
    {{ dbt.safe_cast('ICD_PRCDR_CD22', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd22,
    {{ dbt.safe_cast('PRCDR_DT22', api.Column.translate_type("string")) }}                                                                   as prcdr_dt22,
    {{ dbt.safe_cast('ICD_PRCDR_CD23', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd23,
    {{ dbt.safe_cast('PRCDR_DT23', api.Column.translate_type("string")) }}                                                                   as prcdr_dt23,
    {{ dbt.safe_cast('ICD_PRCDR_CD24', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd24,
    {{ dbt.safe_cast('PRCDR_DT24', api.Column.translate_type("string")) }}                                                                   as prcdr_dt24,
    {{ dbt.safe_cast('ICD_PRCDR_CD25', api.Column.translate_type("string")) }}                                                               as icd_prcdr_cd25,
    {{ dbt.safe_cast('PRCDR_DT25', api.Column.translate_type("string")) }}                                                                   as prcdr_dt25,


    {{ dbt.safe_cast('NCH_PROFNL_CMPNT_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                 as nch_profnl_cmpnt_chrg_amt,



  
    {{ dbt.safe_cast('FI_CLM_ACTN_CD', api.Column.translate_type("string")) }}                                                               as fi_clm_actn_cd,
    {{ dbt.safe_cast('CLM_PPS_IND_CD', api.Column.translate_type("string")) }}                                                               as clm_pps_ind_cd,
    {{ dbt.safe_cast('CLM_IP_ADMSN_TYPE_CD', api.Column.translate_type("string")) }}                                                         as clm_ip_admsn_type_cd,
    {{ dbt.safe_cast('CLM_SRC_IP_ADMSN_CD', api.Column.translate_type("string")) }}                                                          as clm_src_ip_admsn_cd,
    {{ dbt.safe_cast('NCH_PTNT_STATUS_IND_CD', api.Column.translate_type("string")) }}                                                       as nch_ptnt_status_ind_cd,
    {{ dbt.safe_cast('CLM_PASS_THRU_PER_DIEM_AMT', api.Column.translate_type("numeric")) }}                                                as clm_pass_thru_per_diem_amt,
    {{ dbt.safe_cast('NCH_BENE_IP_DDCTBL_AMT', api.Column.translate_type("numeric")) }}                                                    as nch_bene_ip_ddctbl_amt,
    {{ dbt.safe_cast('NCH_BENE_PTA_COINSRNC_LBLTY_AM', api.Column.translate_type("numeric")) }}                                            as nch_bene_pta_coinsrnc_lblty_am,
    {{ dbt.safe_cast('NCH_IP_NCVRD_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                     as nch_ip_ncvrd_chrg_amt,
    {{ dbt.safe_cast('NCH_IP_TOT_DDCTN_AMT', api.Column.translate_type("numeric")) }}                                                      as nch_ip_tot_ddctn_amt,
    {{ dbt.safe_cast('CLM_TOT_PPS_CPTL_AMT', api.Column.translate_type("numeric")) }}                                                      as clm_tot_pps_cptl_amt,

    
    {{ dbt.safe_cast('CLM_PPS_CPTL_FSP_AMT', api.Column.translate_type("numeric")) }}                                                      as clm_pps_cptl_fsp_amt,
    {{ dbt.safe_cast('CLM_PPS_CPTL_OUTLIER_AMT', api.Column.translate_type("numeric")) }}                                                  as clm_pps_cptl_outlier_amt,
    {{ dbt.safe_cast('CLM_PPS_CPTL_DSPRPRTNT_SHR_AMT', api.Column.translate_type("numeric")) }}                                            as clm_pps_cptl_dsprprtnt_shr_amt,
    {{ dbt.safe_cast('CLM_PPS_CPTL_IME_AMT', api.Column.translate_type("numeric")) }}                                                      as clm_pps_cptl_ime_amt,
    {{ dbt.safe_cast('CLM_PPS_CPTL_EXCPTN_AMT', api.Column.translate_type("numeric")) }}                                                   as clm_pps_cptl_excptn_amt,
    {{ dbt.safe_cast('CLM_PPS_OLD_CPTL_HLD_HRMLS_AMT', api.Column.translate_type("numeric")) }}                                            as clm_pps_old_cptl_hld_hrmls_amt,
    {{ dbt.safe_cast('NCH_DRG_OUTLIER_APRVD_PMT_AMT', api.Column.translate_type("numeric")) }}                                             as nch_drg_outlier_aprvd_pmt_amt,
    {{ dbt.safe_cast('CLM_UNCOMPD_CARE_PMT_AMT', api.Column.translate_type("numeric")) }}                                                  as clm_uncompd_care_pmt_amt,
    {{ dbt.safe_cast('IME_OP_CLM_VAL_AMT', api.Column.translate_type("numeric")) }}                                                        as ime_op_clm_val_amt,
    {{ dbt.safe_cast('DSH_OP_CLM_VAL_AMT', api.Column.translate_type("numeric")) }}                                                        as dsh_op_clm_val_amt,


    {{ dbt.safe_cast('CLM_PPS_CPTL_DRG_WT_NUM', api.Column.translate_type("string")) }}                                                      as clm_pps_cptl_drg_wt_num,
    {{ dbt.safe_cast('CLM_UTLZTN_DAY_CNT', api.Column.translate_type("string")) }}                                                           as clm_utlztn_day_cnt,
    {{ dbt.safe_cast('BENE_TOT_COINSRNC_DAYS_CNT', api.Column.translate_type("string")) }}                                                   as bene_tot_coinsrnc_days_cnt,
    {{ dbt.safe_cast('BENE_LRD_USED_CNT', api.Column.translate_type("string")) }}                                                            as bene_lrd_used_cnt,
    {{ dbt.safe_cast('CLM_NON_UTLZTN_DAYS_CNT', api.Column.translate_type("string")) }}                                                      as clm_non_utlztn_days_cnt,
    {{ dbt.safe_cast('NCH_BLOOD_PNTS_FRNSHD_QTY', api.Column.translate_type("string")) }}                                                    as nch_blood_pnts_frnshd_qty,
    {{ dbt.safe_cast('CLM_DRG_CD', api.Column.translate_type("string")) }}                                                                   as clm_drg_cd,
    {{ dbt.safe_cast('CLM_DRG_OUTLIER_STAY_CD', api.Column.translate_type("string")) }}                                                      as clm_drg_outlier_stay_cd,
    {{ dbt.safe_cast('REV_CNTR_DDCTBL_COINSRNC_CD', api.Column.translate_type("string")) }}                                                  as rev_cntr_ddctbl_coinsrnc_cd,


    {{ dbt.safe_cast('ADMTG_DGNS_CD', api.Column.translate_type("string")) }}                                                                as admtg_dgns_cd,
    {{ dbt.safe_cast('CLM_POA_IND_SW1', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw1,
    {{ dbt.safe_cast('CLM_POA_IND_SW2', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw2,
    {{ dbt.safe_cast('CLM_POA_IND_SW3', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw3,
    {{ dbt.safe_cast('CLM_POA_IND_SW4', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw4,
    {{ dbt.safe_cast('CLM_POA_IND_SW5', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw5,
    {{ dbt.safe_cast('CLM_POA_IND_SW6', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw6,
    {{ dbt.safe_cast('CLM_POA_IND_SW7', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw7,
    {{ dbt.safe_cast('CLM_POA_IND_SW8', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw8,
    {{ dbt.safe_cast('CLM_POA_IND_SW9', api.Column.translate_type("string")) }}                                                              as clm_poa_ind_sw9,
    {{ dbt.safe_cast('CLM_POA_IND_SW10', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw10,
    {{ dbt.safe_cast('CLM_POA_IND_SW11', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw11,
    {{ dbt.safe_cast('CLM_POA_IND_SW12', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw12,
    {{ dbt.safe_cast('CLM_POA_IND_SW13', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw13,
    {{ dbt.safe_cast('CLM_POA_IND_SW14', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw14,
    {{ dbt.safe_cast('CLM_POA_IND_SW15', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw15,
    {{ dbt.safe_cast('CLM_POA_IND_SW16', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw16,
    {{ dbt.safe_cast('CLM_POA_IND_SW17', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw17,
    {{ dbt.safe_cast('CLM_POA_IND_SW18', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw18,
    {{ dbt.safe_cast('CLM_POA_IND_SW19', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw19,
    {{ dbt.safe_cast('CLM_POA_IND_SW20', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw20,
    {{ dbt.safe_cast('CLM_POA_IND_SW21', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw21,
    {{ dbt.safe_cast('CLM_POA_IND_SW22', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw22,
    {{ dbt.safe_cast('CLM_POA_IND_SW23', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw23,
    {{ dbt.safe_cast('CLM_POA_IND_SW24', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw24,
    {{ dbt.safe_cast('CLM_POA_IND_SW25', api.Column.translate_type("string")) }}                                                             as clm_poa_ind_sw25,


    {{ dbt.safe_cast('CLM_E_POA_IND_SW1', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw1,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW2', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw2,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW3', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw3,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW4', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw4,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW5', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw5,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW6', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw6,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW7', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw7,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW8', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw8,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW9', api.Column.translate_type("string")) }}                                                            as clm_e_poa_ind_sw9,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW10', api.Column.translate_type("string")) }}                                                           as clm_e_poa_ind_sw10,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW11', api.Column.translate_type("string")) }}                                                           as clm_e_poa_ind_sw11,
    {{ dbt.safe_cast('CLM_E_POA_IND_SW12', api.Column.translate_type("string")) }}                                                           as clm_e_poa_ind_sw12

from inpatient_data
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}