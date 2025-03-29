{{
    config(
        materialized='view'
    )
}}

with int_dme as (
    select *
    from {{ ref('stg_dme') }}
), 
icd_10_codes as (
    select * from {{ ref('int_icd_10_codes') }}
), 
hcpcs_codes as (
    select *,
           row_number() over (
               partition by cpt_hcpcs_code, betos_code 
               order by case when cpt_hcpcs_code is not null then 1 else 2 end
           ) as rn
    from {{ ref('int_cpt_hcpcs_codes') }}
)
select
    int_dme.bene_id,
    int_dme.clm_id,
    int_dme.nch_near_line_rec_ident_cd,
    int_dme.nch_clm_type_cd,
    {{ nch_clm_type_description('nch_clm_type_cd','nch_wkly_proc_dt') }} as claim_type_desc,
    int_dme.clm_from_dt,
    int_dme.clm_thru_dt,
    int_dme.nch_wkly_proc_dt,
    int_dme.carr_clm_entry_cd,
    int_dme.clm_disp_cd,
    int_dme.carr_num,
    int_dme.carr_clm_pmt_dnl_cd,
    int_dme.clm_pmt_amt,
    int_dme.carr_clm_prmry_pyr_pd_amt,
    int_dme.carr_clm_prvdr_asgnmt_ind_sw,
    int_dme.nch_clm_prvdr_pmt_amt,
    int_dme.nch_clm_bene_pmt_amt,
    int_dme.nch_carr_clm_sbmtd_chrg_amt,
    int_dme.nch_carr_clm_alowd_amt,
    int_dme.carr_clm_cash_ddctbl_apld_amt,
    int_dme.carr_clm_hcpcs_yr_cd,

    int_dme.prncpal_dgns_cd,
    int_dme.prncpal_dgns_vrsn_cd,
    prncpal_dgns_code.Short_Description as prncpal_dgns_code_desc,
    int_dme.icd_dgns_cd1,
    int_dme.icd_dgns_vrsn_cd1,
    icd_dgns_1_code.Short_Description as icd_dgns_1_code_desc,
    int_dme.icd_dgns_cd2,
    int_dme.icd_dgns_vrsn_cd2,
    icd_dgns_2_code.Short_Description as icd_dgns_2_code_desc, 
    int_dme.icd_dgns_cd3,
    int_dme.icd_dgns_vrsn_cd3,
    icd_dgns_3_code.Short_Description as icd_dgns_3_code_desc, 
    int_dme.icd_dgns_cd4,
    int_dme.icd_dgns_vrsn_cd4,
    icd_dgns_4_code.Short_Description as icd_dgns_4_code_desc, 
    int_dme.icd_dgns_cd5,
    int_dme.icd_dgns_vrsn_cd5,
    icd_dgns_5_code.Short_Description as icd_dgns_5_code_desc, 
    int_dme.icd_dgns_cd6,
    int_dme.icd_dgns_vrsn_cd6,
    icd_dgns_6_code.Short_Description as icd_dgns_6_code_desc,
    int_dme.icd_dgns_cd7,
    int_dme.icd_dgns_vrsn_cd7,
    icd_dgns_7_code.Short_Description as icd_dgns_7_code_desc,
    int_dme.icd_dgns_cd8,
    int_dme.icd_dgns_vrsn_cd8,
    icd_dgns_8_code.Short_Description as icd_dgns_8_code_desc, 
    int_dme.icd_dgns_cd9,
    int_dme.icd_dgns_vrsn_cd9,
    icd_dgns_9_code.Short_Description as icd_dgns_9_code_desc, 
    int_dme.icd_dgns_cd10,
    int_dme.icd_dgns_vrsn_cd10,
    icd_dgns_10_code.Short_Description as icd_dgns_10_code_desc,
    int_dme.icd_dgns_cd11,
    int_dme.icd_dgns_vrsn_cd11,
    icd_dgns_11_code.Short_Description as icd_dgns_11_code_desc,
    int_dme.icd_dgns_cd12,
    int_dme.icd_dgns_vrsn_cd12,
    icd_dgns_12_code.Short_Description as icd_dgns_12_code_desc,
    int_dme.rfr_physn_upin,
    int_dme.rfr_physn_npi,
    int_dme.clm_clncl_tril_num,
    int_dme.line_num,
    int_dme.tax_num,
    int_dme.prvdr_spclty,
    int_dme.prtcptng_ind_cd,
    int_dme.line_srvc_cnt,
    int_dme.line_cms_type_srvc_cd,
    {{ line_cms_type_srvc_cd_description('line_cms_type_srvc_cd') }} as line_type_srvc_cd_desc,
    int_dme.line_place_of_srvc_cd,
    line_place_of_srvc_codes.Description as line_place_of_srvc_cd_desc,
    int_dme.line_1st_expns_dt,
    int_dme.line_last_expns_dt,
    int_dme.hcpcs_cd,
    hcpcs_code.short_desc as hcpcs_cd_desc,
    int_dme.hcpcs_1st_mdfr_cd,
    hcpcs_code_1st_mdfr.short_desc as hcpcs_1st_mdfr_desc,
    int_dme.hcpcs_2nd_mdfr_cd,
    hcpcs_code_2nd_mdfr.short_desc as hcpcs_2nd_mdfr_desc,
    int_dme.betos_cd,
    int_dme.line_nch_pmt_amt,
    int_dme.line_bene_pmt_amt,
    int_dme.line_prvdr_pmt_amt,
    int_dme.line_bene_ptb_ddctbl_amt,
    int_dme.line_bene_prmry_pyr_cd,
    int_dme.line_bene_prmry_pyr_pd_amt,
    int_dme.line_prmry_alowd_chrg_amt,
    int_dme.line_sbmtd_chrg_amt,
    int_dme.line_alowd_chrg_amt,
    int_dme.line_prcsg_ind_cd,
    int_dme.line_pmt_80_100_cd,
    int_dme.line_service_deductible,
    int_dme.line_icd_dgns_vrsn_cd,
    int_dme.prvdr_num,
    int_dme.prvdr_npi,
    int_dme.dmerc_line_prcng_state_cd,
    int_dme.prvdr_state_cd,
    {{ state_description('prvdr_state_cd') }} as prvdr_state,
    int_dme.dmerc_line_supplr_type_cd,
    int_dme.hcpcs_3rd_mdfr_cd,
    hcpcs_code_3rd_mdfr.short_desc as hcpcs_3rd_mdfr_desc,
    int_dme.hcpcs_4th_mdfr_cd,
    hcpcs_code_4th_mdfr.short_desc as hcpcs_4th_mdfr_desc,
    int_dme.dmerc_line_scrn_svgs_amt,
    int_dme.dmerc_line_mtus_cnt,
    int_dme.dmerc_line_mtus_cd,
    int_dme.line_hct_hgb_rslt_num,
    int_dme.line_ndc_cd

from int_dme
left join icd_10_codes as prncpal_dgns_code
on int_dme.prncpal_dgns_cd = prncpal_dgns_code.ICD_Code

left join icd_10_codes as icd_dgns_1_code
on int_dme.icd_dgns_cd1 = icd_dgns_1_code.ICD_Code
left join icd_10_codes as icd_dgns_2_code
on int_dme.icd_dgns_cd2 = icd_dgns_2_code.ICD_Code
left join icd_10_codes as icd_dgns_3_code
on int_dme.icd_dgns_cd3 = icd_dgns_3_code.ICD_Code
left join icd_10_codes as icd_dgns_4_code
on int_dme.icd_dgns_cd4 = icd_dgns_4_code.ICD_Code
left join icd_10_codes as icd_dgns_5_code
on int_dme.icd_dgns_cd5 = icd_dgns_5_code.ICD_Code
left join icd_10_codes as icd_dgns_6_code
on int_dme.icd_dgns_cd6 = icd_dgns_6_code.ICD_Code
left join icd_10_codes as icd_dgns_7_code
on int_dme.icd_dgns_cd7 = icd_dgns_7_code.ICD_Code
left join icd_10_codes as icd_dgns_8_code
on int_dme.icd_dgns_cd8 = icd_dgns_8_code.ICD_Code
left join icd_10_codes as icd_dgns_9_code
on int_dme.icd_dgns_cd9 = icd_dgns_9_code.ICD_Code
left join icd_10_codes as icd_dgns_10_code
on int_dme.icd_dgns_cd10 = icd_dgns_10_code.ICD_Code
left join icd_10_codes as icd_dgns_11_code
on int_dme.icd_dgns_cd11 = icd_dgns_11_code.ICD_Code
left join icd_10_codes as icd_dgns_12_code
on int_dme.icd_dgns_cd12 = icd_dgns_12_code.ICD_Code

left join {{ ref('line_place_of_srvc_codes') }} as line_place_of_srvc_codes
on int_dme.line_place_of_srvc_cd = CAST(line_place_of_srvc_codes.Code AS STRING)

left join hcpcs_codes as hcpcs_code
  on int_dme.hcpcs_cd = coalesce(hcpcs_code.cpt_hcpcs_code, hcpcs_code.betos_code)
  and hcpcs_code.rn = 1

left join hcpcs_codes as hcpcs_code_1st_mdfr
  on int_dme.hcpcs_1st_mdfr_cd = coalesce(hcpcs_code_1st_mdfr.cpt_hcpcs_code, hcpcs_code_1st_mdfr.betos_code)
  and hcpcs_code_1st_mdfr.rn = 1

left join hcpcs_codes as hcpcs_code_2nd_mdfr
  on int_dme.hcpcs_2nd_mdfr_cd = coalesce(hcpcs_code_2nd_mdfr.cpt_hcpcs_code, hcpcs_code_2nd_mdfr.betos_code)
  and hcpcs_code_2nd_mdfr.rn = 1

left join hcpcs_codes as hcpcs_code_3rd_mdfr
  on int_dme.hcpcs_3rd_mdfr_cd = coalesce(hcpcs_code_3rd_mdfr.cpt_hcpcs_code, hcpcs_code_3rd_mdfr.betos_code)
  and hcpcs_code_1st_mdfr.rn = 1

left join hcpcs_codes as hcpcs_code_4th_mdfr
  on int_dme.hcpcs_4th_mdfr_cd = coalesce(hcpcs_code_4th_mdfr.cpt_hcpcs_code, hcpcs_code_4th_mdfr.betos_code)
  and hcpcs_code_2nd_mdfr.rn = 1  