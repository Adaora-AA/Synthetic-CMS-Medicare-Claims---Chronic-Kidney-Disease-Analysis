{{
    config(
        materialized='view'
    )
}}

with int_hospice as (
    select *
    from {{ ref('stg_hospice') }}
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
    int_hospice.bene_id,
    int_hospice.clm_id,
    int_hospice.nch_near_line_rec_ident_cd,
    int_hospice.nch_clm_type_cd,
    {{ nch_clm_type_description('nch_clm_type_cd','nch_wkly_proc_dt') }} as claim_type_desc,
    int_hospice.clm_from_dt,
    int_hospice.clm_thru_dt,
    int_hospice.nch_wkly_proc_dt,
    int_hospice.fi_clm_proc_dt,
    int_hospice.prvdr_num,
    int_hospice.clm_fac_type_cd,
    type_of_bill_codes.facility_type as facility_type,
    int_hospice.clm_srvc_clsfctn_type_cd,
    type_of_bill_codes.claim_service_type as claim_service_type,
    int_hospice.clm_freq_cd,
    int_hospice.fi_num,
    int_hospice.clm_mdcr_non_pmt_rsn_cd,
    int_hospice.clm_pmt_amt,
    int_hospice.nch_prmry_pyr_clm_pd_amt,
    int_hospice.nch_prmry_pyr_cd,
    int_hospice.prvdr_state_cd,
    {{ state_description('prvdr_state_cd') }} as prvdr_state,
    int_hospice.org_npi_num,
    int_hospice.at_physn_upin,
    int_hospice.at_physn_npi,
    int_hospice.ptnt_dschrg_stus_cd,  
    {{ ptnt_dschrg_stus_cd_desc('ptnt_dschrg_stus_cd') }} as patient_discharge_status,
    int_hospice.clm_tot_chrg_amt,
    int_hospice.nch_ptnt_status_ind_cd,
    {{ ptnt_dschrg_stus_ind_desc('nch_ptnt_status_ind_cd') }} as patient_discharge_ind,
    int_hospice.clm_utlztn_day_cnt,
    int_hospice.nch_bene_dschrg_dt,

    int_hospice.prncpal_dgns_cd,  
    prncpal_dgns_code.Short_Description as prncpal_dgns_code_desc,
    int_hospice.icd_dgns_cd1,
    icd_dgns_1_code.Short_Description as icd_dgns_1_code_desc,
    int_hospice.icd_dgns_cd2,
    icd_dgns_2_code.Short_Description as icd_dgns_2_code_desc, 
    int_hospice.icd_dgns_cd3,
    icd_dgns_3_code.Short_Description as icd_dgns_3_code_desc, 
    int_hospice.icd_dgns_cd4,
    icd_dgns_4_code.Short_Description as icd_dgns_4_code_desc, 
    int_hospice.icd_dgns_cd5,
    icd_dgns_5_code.Short_Description as icd_dgns_5_code_desc, 
    int_hospice.icd_dgns_cd6,
    icd_dgns_6_code.Short_Description as icd_dgns_6_code_desc,
    int_hospice.icd_dgns_cd7,
    icd_dgns_7_code.Short_Description as icd_dgns_7_code_desc,
    int_hospice.icd_dgns_cd8,
    icd_dgns_8_code.Short_Description as icd_dgns_8_code_desc, 
    int_hospice.icd_dgns_cd9,
    icd_dgns_9_code.Short_Description as icd_dgns_9_code_desc, 
    int_hospice.icd_dgns_cd10,
    icd_dgns_10_code.Short_Description as icd_dgns_10_code_desc,
    int_hospice.icd_dgns_cd11,
    icd_dgns_11_code.Short_Description as icd_dgns_11_code_desc,
    int_hospice.icd_dgns_cd12,
    icd_dgns_12_code.Short_Description as icd_dgns_12_code_desc,
    int_hospice.icd_dgns_cd13,
    icd_dgns_13_code.Short_Description as icd_dgns_13_code_desc,
    int_hospice.icd_dgns_cd14,
    icd_dgns_14_code.Short_Description as icd_dgns_14_code_desc,
    int_hospice.icd_dgns_cd15,
    icd_dgns_15_code.Short_Description as icd_dgns_15_code_desc,
    int_hospice.icd_dgns_cd16,
    icd_dgns_16_code.Short_Description as icd_dgns_16_code_desc,
    int_hospice.icd_dgns_cd17,
    icd_dgns_17_code.Short_Description as icd_dgns_17_code_desc,
    int_hospice.icd_dgns_cd18,
    icd_dgns_18_code.Short_Description as icd_dgns_18_code_desc,
    int_hospice.icd_dgns_cd19,
    icd_dgns_19_code.Short_Description as icd_dgns_19_code_desc,
    int_hospice.icd_dgns_cd20,
    icd_dgns_20_code.Short_Description as icd_dgns_20_code_desc,
    int_hospice.icd_dgns_cd21,
    icd_dgns_21_code.Short_Description as icd_dgns_21_code_desc,
    int_hospice.icd_dgns_cd22,
    icd_dgns_22_code.Short_Description as icd_dgns_22_code_desc,
    int_hospice.icd_dgns_cd23,
    icd_dgns_23_code.Short_Description as icd_dgns_23_code_desc,
    int_hospice.icd_dgns_cd24,
    icd_dgns_24_code.Short_Description as icd_dgns_24_code_desc,
    int_hospice.icd_dgns_cd25,
    icd_dgns_25_code.Short_Description as icd_dgns_25_code_desc,

    int_hospice.fst_dgns_e_cd,
    fst_dgns_e_code.Short_Description as fst_dgns_e_code_desc,
    int_hospice.icd_dgns_e_cd1,
    icd_dgns_e_1_code.Short_Description as icd_dgns_e_1_code_desc, 
    int_hospice.icd_dgns_e_cd2,
    icd_dgns_e_2_code.Short_Description as icd_dgns_e_2_code_desc,    
    int_hospice.icd_dgns_e_cd3,
    icd_dgns_e_3_code.Short_Description as icd_dgns_e_3_code_desc, 
    int_hospice.icd_dgns_e_cd4,
    icd_dgns_e_4_code.Short_Description as icd_dgns_e_4_code_desc, 
    int_hospice.icd_dgns_e_cd5,
    icd_dgns_e_5_code.Short_Description as icd_dgns_e_5_code_desc,
    int_hospice.icd_dgns_e_cd6,
    icd_dgns_e_6_code.Short_Description as icd_dgns_e_6_code_desc,
    int_hospice.icd_dgns_e_cd7,
    icd_dgns_e_7_code.Short_Description as icd_dgns_e_7_code_desc, 
    int_hospice.icd_dgns_e_cd8,
    icd_dgns_e_8_code.Short_Description as icd_dgns_e_8_code_desc, 
    int_hospice.icd_dgns_e_cd9,
    icd_dgns_e_9_code.Short_Description as icd_dgns_e_9_code_desc,
    int_hospice.icd_dgns_e_cd10,
    icd_dgns_e_10_code.Short_Description as icd_dgns_e_10_code_desc,
    int_hospice.icd_dgns_e_cd11,
    icd_dgns_e_11_code.Short_Description as icd_dgns_e_11_code_desc, 
    int_hospice.icd_dgns_e_cd12,
    icd_dgns_e_12_code.Short_Description as icd_dgns_e_12_code_desc,    

    int_hospice.clm_hospc_start_dt_id,
    int_hospice.bene_hospc_prd_cnt,
    int_hospice.claim_query_code,
    int_hospice.clm_line_num,
    int_hospice.rev_cntr,
    int_hospice.rev_cntr_dt,
    int_hospice.hcpcs_cd,
    hcpcs_code.short_desc as hcpcs_cd_desc,
    int_hospice.hcpcs_1st_mdfr_cd,
    hcpcs_code_1st_mdfr.short_desc as hcpcs_1st_mdfr_desc,
    int_hospice.hcpcs_2nd_mdfr_cd,
    hcpcs_code_2nd_mdfr.short_desc as hcpcs_2nd_mdfr_desc,
    int_hospice.rev_cntr_unit_cnt,
    int_hospice.rev_cntr_rate_amt,
    int_hospice.rev_cntr_prvdr_pmt_amt,
    int_hospice.rev_cntr_bene_pmt_amt,
    int_hospice.rev_cntr_pmt_amt_amt,
    int_hospice.rev_cntr_tot_chrg_amt,
    int_hospice.rev_cntr_ncvrd_chrg_amt,
    int_hospice.rev_cntr_ddctbl_coinsrnc_cd,
    int_hospice.rev_cntr_ndc_qty,
    int_hospice.rev_cntr_ndc_qty_qlfr_cd,
    int_hospice.rndrng_physn_upin,
    int_hospice.rndrng_physn_npi


from int_hospice
left join icd_10_codes as prncpal_dgns_code
on int_hospice.prncpal_dgns_cd = prncpal_dgns_code.ICD_Code

left join icd_10_codes as icd_dgns_1_code
on int_hospice.icd_dgns_cd1 = icd_dgns_1_code.ICD_Code
left join icd_10_codes as icd_dgns_2_code
on int_hospice.icd_dgns_cd2 = icd_dgns_2_code.ICD_Code
left join icd_10_codes as icd_dgns_3_code
on int_hospice.icd_dgns_cd3 = icd_dgns_3_code.ICD_Code
left join icd_10_codes as icd_dgns_4_code
on int_hospice.icd_dgns_cd4 = icd_dgns_4_code.ICD_Code
left join icd_10_codes as icd_dgns_5_code
on int_hospice.icd_dgns_cd5 = icd_dgns_5_code.ICD_Code
left join icd_10_codes as icd_dgns_6_code
on int_hospice.icd_dgns_cd6 = icd_dgns_6_code.ICD_Code
left join icd_10_codes as icd_dgns_7_code
on int_hospice.icd_dgns_cd7 = icd_dgns_7_code.ICD_Code
left join icd_10_codes as icd_dgns_8_code
on int_hospice.icd_dgns_cd8 = icd_dgns_8_code.ICD_Code
left join icd_10_codes as icd_dgns_9_code
on int_hospice.icd_dgns_cd9 = icd_dgns_9_code.ICD_Code
left join icd_10_codes as icd_dgns_10_code
on int_hospice.icd_dgns_cd10 = icd_dgns_10_code.ICD_Code
left join icd_10_codes as icd_dgns_11_code
on int_hospice.icd_dgns_cd11 = icd_dgns_11_code.ICD_Code
left join icd_10_codes as icd_dgns_12_code
on int_hospice.icd_dgns_cd12 = icd_dgns_12_code.ICD_Code
left join icd_10_codes as icd_dgns_13_code
on int_hospice.icd_dgns_cd13 = icd_dgns_13_code.ICD_Code
left join icd_10_codes as icd_dgns_14_code
on int_hospice.icd_dgns_cd14 = icd_dgns_14_code.ICD_Code
left join icd_10_codes as icd_dgns_15_code
on int_hospice.icd_dgns_cd15 = icd_dgns_15_code.ICD_Code
left join icd_10_codes as icd_dgns_16_code
on int_hospice.icd_dgns_cd16 = icd_dgns_16_code.ICD_Code
left join icd_10_codes as icd_dgns_17_code
on int_hospice.icd_dgns_cd17 = icd_dgns_17_code.ICD_Code
left join icd_10_codes as icd_dgns_18_code
on int_hospice.icd_dgns_cd18 = icd_dgns_18_code.ICD_Code
left join icd_10_codes as icd_dgns_19_code
on int_hospice.icd_dgns_cd19 = icd_dgns_19_code.ICD_Code
left join icd_10_codes as icd_dgns_20_code
on int_hospice.icd_dgns_cd20 = icd_dgns_20_code.ICD_Code
left join icd_10_codes as icd_dgns_21_code
on int_hospice.icd_dgns_cd21 = icd_dgns_21_code.ICD_Code
left join icd_10_codes as icd_dgns_22_code
on int_hospice.icd_dgns_cd22 = icd_dgns_22_code.ICD_Code
left join icd_10_codes as icd_dgns_23_code
on int_hospice.icd_dgns_cd23 = icd_dgns_23_code.ICD_Code
left join icd_10_codes as icd_dgns_24_code
on int_hospice.icd_dgns_cd24 = icd_dgns_24_code.ICD_Code
left join icd_10_codes as icd_dgns_25_code
on int_hospice.icd_dgns_cd25 = icd_dgns_25_code.ICD_Code


left join icd_10_codes as fst_dgns_e_code
on int_hospice.fst_dgns_e_cd = fst_dgns_e_code.ICD_Code

left join icd_10_codes as icd_dgns_e_1_code
on int_hospice.icd_dgns_e_cd1 = icd_dgns_e_1_code.ICD_Code
left join icd_10_codes as icd_dgns_e_2_code
on int_hospice.icd_dgns_e_cd2 = icd_dgns_e_2_code.ICD_Code
left join icd_10_codes as icd_dgns_e_3_code
on int_hospice.icd_dgns_e_cd3 = icd_dgns_e_3_code.ICD_Code
left join icd_10_codes as icd_dgns_e_4_code
on int_hospice.icd_dgns_e_cd4 = icd_dgns_e_4_code.ICD_Code
left join icd_10_codes as icd_dgns_e_5_code
on int_hospice.icd_dgns_e_cd5 = icd_dgns_e_5_code.ICD_Code
left join icd_10_codes as icd_dgns_e_6_code
on int_hospice.icd_dgns_e_cd6 = icd_dgns_e_6_code.ICD_Code
left join icd_10_codes as icd_dgns_e_7_code
on int_hospice.icd_dgns_e_cd7 = icd_dgns_e_7_code.ICD_Code
left join icd_10_codes as icd_dgns_e_8_code
on int_hospice.icd_dgns_e_cd8 = icd_dgns_e_8_code.ICD_Code
left join icd_10_codes as icd_dgns_e_9_code
on int_hospice.icd_dgns_e_cd9 = icd_dgns_e_9_code.ICD_Code
left join icd_10_codes as icd_dgns_e_10_code
on int_hospice.icd_dgns_e_cd10 = icd_dgns_e_10_code.ICD_Code
left join icd_10_codes as icd_dgns_e_11_code
on int_hospice.icd_dgns_e_cd11 = icd_dgns_e_11_code.ICD_Code
left join icd_10_codes as icd_dgns_e_12_code
on int_hospice.icd_dgns_e_cd12 = icd_dgns_e_12_code.ICD_Code


LEFT JOIN {{ ref('type_of_bill_codes') }} AS type_of_bill_codes 
ON int_hospice.clm_fac_type_cd = type_of_bill_codes.facilty_type_cd 
AND int_hospice.clm_srvc_clsfctn_type_cd = type_of_bill_codes.claim_service_type_cd

left join hcpcs_codes as hcpcs_code
  on int_hospice.hcpcs_cd = coalesce(hcpcs_code.cpt_hcpcs_code, hcpcs_code.betos_code)
  and hcpcs_code.rn = 1

left join hcpcs_codes as hcpcs_code_1st_mdfr
  on int_hospice.hcpcs_1st_mdfr_cd = coalesce(hcpcs_code_1st_mdfr.cpt_hcpcs_code, hcpcs_code_1st_mdfr.betos_code)
  and hcpcs_code_1st_mdfr.rn = 1

left join hcpcs_codes as hcpcs_code_2nd_mdfr
  on int_hospice.hcpcs_2nd_mdfr_cd = coalesce(hcpcs_code_2nd_mdfr.cpt_hcpcs_code, hcpcs_code_2nd_mdfr.betos_code)
  and hcpcs_code_2nd_mdfr.rn = 1