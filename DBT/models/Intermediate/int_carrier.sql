/*Note that there is a data issue with the incorrect assignment of National Claims History (NCH) claim
type codes for 37,962 Part B carrier and DMERC (claim type codes 71,72,81,82) claims processed on
01/27/23 (i.e., the NCH_WKLY_PROC_DT). For nearly all of the affected claims, the
NCH_CLM_TYPE_CD was incorrectly assigned an 81 instead of 82; there are also 7 of the total
impacted claims where the NCH_CLM_TPYE_CD was incorrectly assigned 71 instead of 72.
*/


{{
    config(
        materialized='view'
    )
}}

with int_carrier as (
    select *
    from {{ ref('stg_carrier') }}
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
    int_carrier.bene_id,
    int_carrier.clm_id,
    int_carrier.nch_near_line_rec_ident_cd,
    int_carrier.nch_clm_type_cd,
    {{ nch_clm_type_description('nch_clm_type_cd','nch_wkly_proc_dt') }} as claim_type_desc,
    int_carrier.clm_from_dt,
    int_carrier.clm_thru_dt,
    int_carrier.nch_wkly_proc_dt,
    int_carrier.carr_clm_entry_cd,
    int_carrier.clm_disp_cd,
    int_carrier.carr_num,
    int_carrier.carr_clm_pmt_dnl_cd,
    int_carrier.clm_pmt_amt,
    int_carrier.carr_clm_prmry_pyr_pd_amt,
    int_carrier.rfr_physn_upin,
    int_carrier.rfr_physn_npi,
    int_carrier.carr_clm_prvdr_asgnmt_ind_sw,
    int_carrier.nch_clm_prvdr_pmt_amt,
    int_carrier.nch_clm_bene_pmt_amt,
    int_carrier.nch_carr_clm_sbmtd_chrg_amt,
    int_carrier.nch_carr_clm_alowd_amt,
    int_carrier.carr_clm_cash_ddctbl_apld_amt,
    int_carrier.carr_clm_hcpcs_yr_cd,
    int_carrier.carr_clm_rfrng_pin_num,
    int_carrier.prncpal_dgns_cd,
    int_carrier.prncpal_dgns_vrsn_cd,
    prncpal_dgns_code.Short_Description as prncpal_dgns_code_desc,
    int_carrier.icd_dgns_cd1,
    int_carrier.icd_dgns_vrsn_cd1,
    icd_dgns_1_code.Short_Description as icd_dgns_1_code_desc,
    int_carrier.icd_dgns_cd2,
    int_carrier.icd_dgns_vrsn_cd2,
    icd_dgns_2_code.Short_Description as icd_dgns_2_code_desc, 
    int_carrier.icd_dgns_cd3,
    int_carrier.icd_dgns_vrsn_cd3,
    icd_dgns_3_code.Short_Description as icd_dgns_3_code_desc, 
    int_carrier.icd_dgns_cd4,
    int_carrier.icd_dgns_vrsn_cd4,
    icd_dgns_4_code.Short_Description as icd_dgns_4_code_desc, 
    int_carrier.icd_dgns_cd5,
    int_carrier.icd_dgns_vrsn_cd5,
    icd_dgns_5_code.Short_Description as icd_dgns_5_code_desc, 
    int_carrier.icd_dgns_cd6,
    int_carrier.icd_dgns_vrsn_cd6,
    icd_dgns_6_code.Short_Description as icd_dgns_6_code_desc,
    int_carrier.icd_dgns_cd7,
    int_carrier.icd_dgns_vrsn_cd7,
    icd_dgns_7_code.Short_Description as icd_dgns_7_code_desc,
    int_carrier.icd_dgns_cd8,
    int_carrier.icd_dgns_vrsn_cd8,
    icd_dgns_8_code.Short_Description as icd_dgns_8_code_desc, 
    int_carrier.icd_dgns_cd9,
    int_carrier.icd_dgns_vrsn_cd9,
    icd_dgns_9_code.Short_Description as icd_dgns_9_code_desc, 
    int_carrier.icd_dgns_cd10,
    int_carrier.icd_dgns_vrsn_cd10,
    icd_dgns_10_code.Short_Description as icd_dgns_10_code_desc,
    int_carrier.icd_dgns_cd11,
    int_carrier.icd_dgns_vrsn_cd11,
    icd_dgns_11_code.Short_Description as icd_dgns_11_code_desc,
    int_carrier.icd_dgns_cd12,
    int_carrier.icd_dgns_vrsn_cd12,
    icd_dgns_12_code.Short_Description as icd_dgns_12_code_desc,
    int_carrier.clm_clncl_tril_num,
    int_carrier.carr_clm_blg_npi_num,
    int_carrier.line_num,
    int_carrier.carr_prfrng_pin_num,
    int_carrier.prf_physn_upin,
    int_carrier.prf_physn_npi,
    int_carrier.org_npi_num,
    int_carrier.carr_line_prvdr_type_cd,
    int_carrier.tax_num,
    int_carrier.prvdr_state_cd,
    {{ state_description('prvdr_state_cd') }} as prvdr_state,
    int_carrier.prvdr_zip,
    int_carrier.prvdr_spclty,
    int_carrier.prtcptng_ind_cd,
    int_carrier.carr_line_rdcd_pmt_phys_astn_c,
    int_carrier.line_srvc_cnt,
    int_carrier.line_cms_type_srvc_cd,
    {{ line_cms_type_srvc_cd_description('line_cms_type_srvc_cd') }} as line_type_srvc_cd_desc,
    int_carrier.line_place_of_srvc_cd,
    line_place_of_srvc_codes.Description as line_place_of_srvc_cd_desc ,
    int_carrier.carr_line_prcng_lclty_cd,
    int_carrier.line_1st_expns_dt,
    int_carrier.line_last_expns_dt,
    int_carrier.hcpcs_cd,
    hcpcs_code.short_desc as hcpcs_cd_desc,
    int_carrier.hcpcs_1st_mdfr_cd,
    hcpcs_code_1st_mdfr.short_desc as hcpcs_1st_mdfr_desc,
    int_carrier.hcpcs_2nd_mdfr_cd,
    hcpcs_code_2nd_mdfr.short_desc as hcpcs_2nd_mdfr_desc,
    int_carrier.betos_cd,
    int_carrier.line_nch_pmt_amt,
    int_carrier.line_bene_pmt_amt,
    int_carrier.line_prvdr_pmt_amt,
    int_carrier.line_bene_ptb_ddctbl_amt,
    int_carrier.line_bene_prmry_pyr_cd,
    int_carrier.line_bene_prmry_pyr_pd_amt,
    int_carrier.line_coinsrnc_amt,
    int_carrier.line_sbmtd_chrg_amt,
    int_carrier.line_alowd_chrg_amt,
    int_carrier.line_prcsg_ind_cd,
    int_carrier.line_pmt_80_100_cd,
    int_carrier.line_service_deductible,
    int_carrier.carr_line_mtus_cnt,
    int_carrier.carr_line_mtus_cd,
    int_carrier.line_icd_dgns_cd,
    int_carrier.line_icd_dgns_vrsn_cd,
    line_icd_dgns_code.Short_Description as line_icd_dgns_code_desc,
    int_carrier.hpsa_scrcty_ind_cd,
    int_carrier.carr_line_rx_num,
    int_carrier.line_hct_hgb_rslt_num,
    int_carrier.line_hct_hgb_type_cd,
    int_carrier.line_ndc_cd,
    int_carrier.carr_line_clia_lab_num,
    int_carrier.carr_line_ansthsa_unit_cnt   

from int_carrier
left join icd_10_codes as prncpal_dgns_code
on int_carrier.prncpal_dgns_cd = prncpal_dgns_code.ICD_Code

left join icd_10_codes as icd_dgns_1_code
on int_carrier.icd_dgns_cd1 = icd_dgns_1_code.ICD_Code
left join icd_10_codes as icd_dgns_2_code
on int_carrier.icd_dgns_cd2 = icd_dgns_2_code.ICD_Code
left join icd_10_codes as icd_dgns_3_code
on int_carrier.icd_dgns_cd3 = icd_dgns_3_code.ICD_Code
left join icd_10_codes as icd_dgns_4_code
on int_carrier.icd_dgns_cd4 = icd_dgns_4_code.ICD_Code
left join icd_10_codes as icd_dgns_5_code
on int_carrier.icd_dgns_cd5 = icd_dgns_5_code.ICD_Code
left join icd_10_codes as icd_dgns_6_code
on int_carrier.icd_dgns_cd6 = icd_dgns_6_code.ICD_Code
left join icd_10_codes as icd_dgns_7_code
on int_carrier.icd_dgns_cd7 = icd_dgns_7_code.ICD_Code
left join icd_10_codes as icd_dgns_8_code
on int_carrier.icd_dgns_cd8 = icd_dgns_8_code.ICD_Code
left join icd_10_codes as icd_dgns_9_code
on int_carrier.icd_dgns_cd9 = icd_dgns_9_code.ICD_Code
left join icd_10_codes as icd_dgns_10_code
on int_carrier.icd_dgns_cd10 = icd_dgns_10_code.ICD_Code
left join icd_10_codes as icd_dgns_11_code
on int_carrier.icd_dgns_cd11 = icd_dgns_11_code.ICD_Code
left join icd_10_codes as icd_dgns_12_code
on int_carrier.icd_dgns_cd12 = icd_dgns_12_code.ICD_Code

left join icd_10_codes as line_icd_dgns_code
on int_carrier.line_icd_dgns_cd = line_icd_dgns_code.ICD_Code

left join {{ ref('line_place_of_srvc_codes') }} as line_place_of_srvc_codes
on int_carrier.line_place_of_srvc_cd = CAST(line_place_of_srvc_codes.Code AS STRING)

left join hcpcs_codes as hcpcs_code
  on int_carrier.hcpcs_cd = coalesce(hcpcs_code.cpt_hcpcs_code, hcpcs_code.betos_code)
  and hcpcs_code.rn = 1

left join hcpcs_codes as hcpcs_code_1st_mdfr
  on int_carrier.hcpcs_1st_mdfr_cd = coalesce(hcpcs_code_1st_mdfr.cpt_hcpcs_code, hcpcs_code_1st_mdfr.betos_code)
  and hcpcs_code_1st_mdfr.rn = 1

left join hcpcs_codes as hcpcs_code_2nd_mdfr
  on int_carrier.hcpcs_2nd_mdfr_cd = coalesce(hcpcs_code_2nd_mdfr.cpt_hcpcs_code, hcpcs_code_2nd_mdfr.betos_code)
  and hcpcs_code_2nd_mdfr.rn = 1