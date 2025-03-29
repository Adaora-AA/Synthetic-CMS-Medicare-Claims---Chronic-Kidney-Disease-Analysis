{{
    config(
        materialized='view'
    )
}}

with carrier_data as 
(
  select *,
    row_number() over(partition by BENE_ID,CLM_ID, LINE_NUM) as rn
  from {{ source('staging','carrier') }}
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

    SAFE.PARSE_DATE('%d-%b-%Y', LINE_1ST_EXPNS_DT) AS line_1st_expns_dt,
    SAFE.PARSE_DATE('%d-%b-%Y', LINE_LAST_EXPNS_DT) AS line_last_expns_dt,


    -- info
    {{ dbt.safe_cast('CLM_PMT_AMT', api.Column.translate_type("numeric")) }}                                                               as clm_pmt_amt,
    {{ dbt.safe_cast('RFR_PHYSN_NPI', api.Column.translate_type("string")) }}                                                                as rfr_physn_npi,
    {{ dbt.safe_cast('NCH_CLM_PRVDR_PMT_AMT', api.Column.translate_type("numeric")) }}                                                     as nch_clm_prvdr_pmt_amt,
    {{ dbt.safe_cast('NCH_CLM_BENE_PMT_AMT', api.Column.translate_type("numeric")) }}                                                      as nch_clm_bene_pmt_amt,
    {{ dbt.safe_cast('NCH_CARR_CLM_SBMTD_CHRG_AMT', api.Column.translate_type("numeric")) }}                                               as nch_carr_clm_sbmtd_chrg_amt,
    {{ dbt.safe_cast('NCH_CARR_CLM_ALOWD_AMT', api.Column.translate_type("numeric")) }}                                                    as nch_carr_clm_alowd_amt,
    {{ dbt.safe_cast('CARR_CLM_HCPCS_YR_CD', api.Column.translate_type("string")) }}                                                         as carr_clm_hcpcs_yr_cd,
    {{ dbt.safe_cast('CARR_CLM_ENTRY_CD', api.Column.translate_type("string")) }}                                                         as carr_clm_entry_cd,

    {{ dbt.safe_cast('PRNCPAL_DGNS_CD', api.Column.translate_type("string")) }}                                                              as prncpal_dgns_cd,
    {{ dbt.safe_cast('PRNCPAL_DGNS_VRSN_CD', api.Column.translate_type("string")) }}                                                         as prncpal_dgns_vrsn_cd,
    {{ dbt.safe_cast('ICD_DGNS_CD1', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd1,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD1', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd1,
    {{ dbt.safe_cast('ICD_DGNS_CD2', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd2,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD2', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd2,
    {{ dbt.safe_cast('ICD_DGNS_CD3', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd3,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD3', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd3,
    {{ dbt.safe_cast('ICD_DGNS_CD4', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd4,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD4', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd4,
    {{ dbt.safe_cast('ICD_DGNS_CD5', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd5,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD5', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd5,
    {{ dbt.safe_cast('ICD_DGNS_CD6', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd6,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD6', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd6,
    {{ dbt.safe_cast('ICD_DGNS_CD7', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd7,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD7', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd7,
    {{ dbt.safe_cast('ICD_DGNS_CD8', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd8,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD8', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd8,
    {{ dbt.safe_cast('ICD_DGNS_CD9', api.Column.translate_type("string")) }}                                                                 as icd_dgns_cd9,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD9', api.Column.translate_type("string")) }}                                                            as icd_dgns_vrsn_cd9,
    {{ dbt.safe_cast('ICD_DGNS_CD10', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd10,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD10', api.Column.translate_type("string")) }}                                                           as icd_dgns_vrsn_cd10,
    {{ dbt.safe_cast('ICD_DGNS_CD11', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd11,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD11', api.Column.translate_type("string")) }}                                                           as icd_dgns_vrsn_cd11,
    {{ dbt.safe_cast('ICD_DGNS_CD12', api.Column.translate_type("string")) }}                                                                as icd_dgns_cd12,
    {{ dbt.safe_cast('ICD_DGNS_VRSN_CD12', api.Column.translate_type("string")) }}                                                           as icd_dgns_vrsn_cd12,

    {{ dbt.safe_cast('LINE_NUM', api.Column.translate_type("numeric")) }}                                                                  as line_num,
    {{ dbt.safe_cast('PRF_PHYSN_NPI', api.Column.translate_type("string")) }}                                                                as prf_physn_npi,
    {{ dbt.safe_cast('ORG_NPI_NUM', api.Column.translate_type("string")) }}                                                                  as org_npi_num,
    {{ dbt.safe_cast('CARR_LINE_PRVDR_TYPE_CD', api.Column.translate_type("string")) }}                                                      as carr_line_prvdr_type_cd,
    {{ dbt.safe_cast('PRVDR_STATE_CD', api.Column.translate_type("string")) }}                                                               as prvdr_state_cd,
    {{ dbt.safe_cast('LINE_SRVC_CNT', api.Column.translate_type("string")) }}                                                                as line_srvc_cnt,
    {{ dbt.safe_cast('LINE_CMS_TYPE_SRVC_CD', api.Column.translate_type("string")) }}                                                        as line_cms_type_srvc_cd,
    {{ dbt.safe_cast('LINE_PLACE_OF_SRVC_CD', api.Column.translate_type("string")) }}                                                        as line_place_of_srvc_cd,
    {{ dbt.safe_cast('HCPCS_CD', api.Column.translate_type("string")) }}                                                                     as hcpcs_cd,
    {{ dbt.safe_cast('HCPCS_1ST_MDFR_CD', api.Column.translate_type("string")) }}                                                            as hcpcs_1st_mdfr_cd,
    {{ dbt.safe_cast('HCPCS_2ND_MDFR_CD', api.Column.translate_type("string")) }}                                                            as hcpcs_2nd_mdfr_cd,
    {{ dbt.safe_cast('LINE_NCH_PMT_AMT', api.Column.translate_type("numeric")) }}                                                          as line_nch_pmt_amt,
    {{ dbt.safe_cast('LINE_BENE_PMT_AMT', api.Column.translate_type("numeric")) }}                                                         as line_bene_pmt_amt,
    {{ dbt.safe_cast('LINE_PRVDR_PMT_AMT', api.Column.translate_type("numeric")) }}                                                        as line_prvdr_pmt_amt, 
    {{ dbt.safe_cast('LINE_ALOWD_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                       as line_alowd_chrg_amt,    
    {{ dbt.safe_cast('LINE_ICD_DGNS_CD', api.Column.translate_type("string")) }}                                                             as line_icd_dgns_cd,
    {{ dbt.safe_cast('LINE_ICD_DGNS_VRSN_CD', api.Column.translate_type("string")) }}                                                        as line_icd_dgns_vrsn_cd,
    


    --others

    
    {{ dbt.safe_cast('CLM_DISP_CD', api.Column.translate_type("string")) }}                                                                  as clm_disp_cd,
    {{ dbt.safe_cast('CARR_NUM', api.Column.translate_type("string")) }}                                                                     as carr_num,
    {{ dbt.safe_cast('CARR_CLM_PMT_DNL_CD', api.Column.translate_type("string")) }}                                                          as carr_clm_pmt_dnl_cd,
    {{ dbt.safe_cast('CARR_CLM_PRMRY_PYR_PD_AMT', api.Column.translate_type("numeric")) }}                                                 as carr_clm_prmry_pyr_pd_amt,
    {{ dbt.safe_cast('RFR_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                               as rfr_physn_upin,
    {{ dbt.safe_cast('CARR_CLM_PRVDR_ASGNMT_IND_SW', api.Column.translate_type("string")) }}                                                 as carr_clm_prvdr_asgnmt_ind_sw,
    {{ dbt.safe_cast('CARR_CLM_CASH_DDCTBL_APLD_AMT', api.Column.translate_type("string")) }}                                                as carr_clm_cash_ddctbl_apld_amt,
    {{ dbt.safe_cast('CARR_CLM_RFRNG_PIN_NUM', api.Column.translate_type("string")) }}                                                       as carr_clm_rfrng_pin_num,
    {{ dbt.safe_cast('CLM_CLNCL_TRIL_NUM', api.Column.translate_type("string")) }}                                                           as clm_clncl_tril_num,

    {{ dbt.safe_cast('CARR_CLM_BLG_NPI_NUM', api.Column.translate_type("string")) }}                                                         as carr_clm_blg_npi_num,
    {{ dbt.safe_cast('CARR_PRFRNG_PIN_NUM', api.Column.translate_type("string")) }}                                                          as carr_prfrng_pin_num,
    {{ dbt.safe_cast('PRF_PHYSN_UPIN', api.Column.translate_type("string")) }}                                                               as prf_physn_upin,
    {{ dbt.safe_cast('TAX_NUM', api.Column.translate_type("string")) }}                                                                      as tax_num,
    {{ dbt.safe_cast('PRVDR_ZIP', api.Column.translate_type("string")) }}                                                                    as prvdr_zip,
    {{ dbt.safe_cast('PRVDR_SPCLTY', api.Column.translate_type("string")) }}                                                                 as prvdr_spclty,
    {{ dbt.safe_cast('PRTCPTNG_IND_CD', api.Column.translate_type("string")) }}                                                              as prtcptng_ind_cd,
    {{ dbt.safe_cast('CARR_LINE_RDCD_PMT_PHYS_ASTN_C', api.Column.translate_type("string")) }}                                               as carr_line_rdcd_pmt_phys_astn_c,
    {{ dbt.safe_cast('CARR_LINE_PRCNG_LCLTY_CD', api.Column.translate_type("string")) }}                                                     as carr_line_prcng_lclty_cd,

    
    {{ dbt.safe_cast('BETOS_CD', api.Column.translate_type("string")) }}                                                                     as betos_cd,
    {{ dbt.safe_cast('LINE_BENE_PTB_DDCTBL_AMT', api.Column.translate_type("numeric")) }}                                                  as line_bene_ptb_ddctbl_amt,
    {{ dbt.safe_cast('LINE_BENE_PRMRY_PYR_CD', api.Column.translate_type("string")) }}                                                       as line_bene_prmry_pyr_cd,
    {{ dbt.safe_cast('LINE_BENE_PRMRY_PYR_PD_AMT', api.Column.translate_type("numeric")) }}                                                as line_bene_prmry_pyr_pd_amt,
    {{ dbt.safe_cast('LINE_COINSRNC_AMT', api.Column.translate_type("numeric")) }}                                                         as line_coinsrnc_amt,
    {{ dbt.safe_cast('LINE_SBMTD_CHRG_AMT', api.Column.translate_type("numeric")) }}                                                       as line_sbmtd_chrg_amt,
    {{ dbt.safe_cast('LINE_PRCSG_IND_CD', api.Column.translate_type("string")) }}                                                            as line_prcsg_ind_cd,
    
    {{ dbt.safe_cast('LINE_PMT_80_100_CD', api.Column.translate_type("string")) }}                                                           as line_pmt_80_100_cd,
    {{ dbt.safe_cast('LINE_SERVICE_DEDUCTIBLE', api.Column.translate_type("string")) }}                                                      as line_service_deductible,
    {{ dbt.safe_cast('CARR_LINE_MTUS_CNT', api.Column.translate_type("string")) }}                                                           as carr_line_mtus_cnt,
    {{ dbt.safe_cast('CARR_LINE_MTUS_CD', api.Column.translate_type("string")) }}                                                            as carr_line_mtus_cd,
    {{ dbt.safe_cast('HPSA_SCRCTY_IND_CD', api.Column.translate_type("string")) }}                                                           as hpsa_scrcty_ind_cd,
    {{ dbt.safe_cast('CARR_LINE_RX_NUM', api.Column.translate_type("bigint")) }}                                                           as carr_line_rx_num,
    {{ dbt.safe_cast('LINE_HCT_HGB_RSLT_NUM', api.Column.translate_type("integer")) }}                                                     as line_hct_hgb_rslt_num,
    {{ dbt.safe_cast('LINE_HCT_HGB_TYPE_CD', api.Column.translate_type("string")) }}                                                         as line_hct_hgb_type_cd,
    {{ dbt.safe_cast('LINE_NDC_CD', api.Column.translate_type("string")) }}                                                                  as line_ndc_cd,
    {{ dbt.safe_cast('CARR_LINE_CLIA_LAB_NUM', api.Column.translate_type("numeric")) }}                                                    as carr_line_clia_lab_num,
    {{ dbt.safe_cast('CARR_LINE_ANSTHSA_UNIT_CNT', api.Column.translate_type("string")) }}                                                   as carr_line_ansthsa_unit_cnt

from carrier_data
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}