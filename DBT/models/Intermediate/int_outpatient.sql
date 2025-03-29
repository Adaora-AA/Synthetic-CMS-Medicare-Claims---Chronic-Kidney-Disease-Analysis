{{
    config(
        materialized='view'
    )
}}

with int_outpatient as (
    select *
    from {{ ref('stg_outpatient') }}
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
    int_outpatient.bene_id,
    int_outpatient.clm_id,
    int_outpatient.nch_near_line_rec_ident_cd,
    int_outpatient.nch_clm_type_cd,
    {{ nch_clm_type_description('nch_clm_type_cd','nch_wkly_proc_dt') }} as claim_type_desc,
    int_outpatient.clm_from_dt,
    int_outpatient.clm_thru_dt,
    int_outpatient.nch_wkly_proc_dt,
    int_outpatient.fi_clm_proc_dt,
    int_outpatient.claim_query_code,
    int_outpatient.prvdr_num,
    int_outpatient.clm_fac_type_cd,
    type_of_bill_codes.facility_type as facility_type,
    int_outpatient.clm_srvc_clsfctn_type_cd,
    type_of_bill_codes.claim_service_type as claim_service_type,
    int_outpatient.clm_freq_cd,
    int_outpatient.fi_num,
    int_outpatient.clm_mdcr_non_pmt_rsn_cd,
    int_outpatient.clm_pmt_amt,
    int_outpatient.nch_prmry_pyr_clm_pd_amt,
    int_outpatient.nch_prmry_pyr_cd,
    int_outpatient.prvdr_state_cd,
    {{ state_description('prvdr_state_cd') }} as prvdr_state,
    int_outpatient.org_npi_num,
    int_outpatient.at_physn_upin,
    int_outpatient.at_physn_npi,
    int_outpatient.op_physn_upin,
    int_outpatient.op_physn_npi,
    int_outpatient.ot_physn_upin,
    int_outpatient.ot_physn_npi,
    int_outpatient.clm_mco_pd_sw, 
    int_outpatient.ptnt_dschrg_stus_cd,
    {{ ptnt_dschrg_stus_cd_desc('ptnt_dschrg_stus_cd') }} as patient_discharge_status,
    int_outpatient.clm_tot_chrg_amt,
    int_outpatient.nch_bene_blood_ddctbl_lblty_am,
    int_outpatient.nch_profnl_cmpnt_chrg_amt,


    int_outpatient.prncpal_dgns_cd,  
    prncpal_dgns_code.Short_Description as prncpal_dgns_code_desc,
    int_outpatient.icd_dgns_cd1,
    icd_dgns_1_code.Short_Description as icd_dgns_1_code_desc,
    int_outpatient.icd_dgns_cd2,
    icd_dgns_2_code.Short_Description as icd_dgns_2_code_desc,
    int_outpatient.icd_dgns_cd3,
    icd_dgns_3_code.Short_Description as icd_dgns_3_code_desc,
    int_outpatient.icd_dgns_cd4,
    icd_dgns_4_code.Short_Description as icd_dgns_4_code_desc,
    int_outpatient.icd_dgns_cd5,
    icd_dgns_5_code.Short_Description as icd_dgns_5_code_desc, 
    int_outpatient.icd_dgns_cd6,
    icd_dgns_6_code.Short_Description as icd_dgns_6_code_desc,
    int_outpatient.icd_dgns_cd7,
    icd_dgns_7_code.Short_Description as icd_dgns_7_code_desc,
    int_outpatient.icd_dgns_cd8,
    icd_dgns_8_code.Short_Description as icd_dgns_8_code_desc,
    int_outpatient.icd_dgns_cd9,
    icd_dgns_9_code.Short_Description as icd_dgns_9_code_desc,
    int_outpatient.icd_dgns_cd10,
    icd_dgns_10_code.Short_Description as icd_dgns_10_code_desc,
    int_outpatient.icd_dgns_cd11,
    icd_dgns_11_code.Short_Description as icd_dgns_11_code_desc,
    int_outpatient.icd_dgns_cd12,
    icd_dgns_12_code.Short_Description as icd_dgns_12_code_desc,
    int_outpatient.icd_dgns_cd13,
    icd_dgns_13_code.Short_Description as icd_dgns_13_code_desc,
    int_outpatient.icd_dgns_cd14,
    icd_dgns_14_code.Short_Description as icd_dgns_14_code_desc,
    int_outpatient.icd_dgns_cd15,
    icd_dgns_15_code.Short_Description as icd_dgns_15_code_desc,
    int_outpatient.icd_dgns_cd16,
    icd_dgns_16_code.Short_Description as icd_dgns_16_code_desc,
    int_outpatient.icd_dgns_cd17,
    icd_dgns_17_code.Short_Description as icd_dgns_17_code_desc,
    int_outpatient.icd_dgns_cd18,
    icd_dgns_18_code.Short_Description as icd_dgns_18_code_desc,
    int_outpatient.icd_dgns_cd19,
    icd_dgns_19_code.Short_Description as icd_dgns_19_code_desc,
    int_outpatient.icd_dgns_cd20,
    icd_dgns_20_code.Short_Description as icd_dgns_20_code_desc,
    int_outpatient.icd_dgns_cd21,
    icd_dgns_21_code.Short_Description as icd_dgns_21_code_desc,
    int_outpatient.icd_dgns_cd22,
    icd_dgns_22_code.Short_Description as icd_dgns_22_code_desc,
    int_outpatient.icd_dgns_cd23,
    icd_dgns_23_code.Short_Description as icd_dgns_23_code_desc,
    int_outpatient.icd_dgns_cd24,
    icd_dgns_24_code.Short_Description as icd_dgns_24_code_desc,
    int_outpatient.icd_dgns_cd25,
    icd_dgns_25_code.Short_Description as icd_dgns_25_code_desc,

    int_outpatient.fst_dgns_e_cd,
    fst_dgns_e_code.Short_Description as fst_dgns_e_code_desc,
    int_outpatient.icd_dgns_e_cd1,
    icd_dgns_e_1_code.Short_Description as icd_dgns_e_1_code_desc, 
    int_outpatient.icd_dgns_e_cd2,
    icd_dgns_e_2_code.Short_Description as icd_dgns_e_2_code_desc, 
    int_outpatient.icd_dgns_e_cd3,
    icd_dgns_e_3_code.Short_Description as icd_dgns_e_3_code_desc,
    int_outpatient.icd_dgns_e_cd4,
    icd_dgns_e_4_code.Short_Description as icd_dgns_e_4_code_desc,
    int_outpatient.icd_dgns_e_cd5,
    icd_dgns_e_5_code.Short_Description as icd_dgns_e_5_code_desc,
    int_outpatient.icd_dgns_e_cd6,
    icd_dgns_e_6_code.Short_Description as icd_dgns_e_6_code_desc,
    int_outpatient.icd_dgns_e_cd7,
    icd_dgns_e_7_code.Short_Description as icd_dgns_e_7_code_desc,
    int_outpatient.icd_dgns_e_cd8,
    icd_dgns_e_8_code.Short_Description as icd_dgns_e_8_code_desc,
    int_outpatient.icd_dgns_e_cd9,
    icd_dgns_e_9_code.Short_Description as icd_dgns_e_9_code_desc,
    int_outpatient.icd_dgns_e_cd10,
    icd_dgns_e_10_code.Short_Description as icd_dgns_e_10_code_desc,
    int_outpatient.icd_dgns_e_cd11,
    icd_dgns_e_11_code.Short_Description as icd_dgns_e_11_code_desc,
    int_outpatient.icd_dgns_e_cd12,
    icd_dgns_e_12_code.Short_Description as icd_dgns_e_12_code_desc,   


    int_outpatient.icd_prcdr_cd1,
    icd_prcdr_1_code.Short_Description as icd_prcdr_1_code_desc,
    int_outpatient.prcdr_dt1,
    int_outpatient.icd_prcdr_cd2,
    icd_prcdr_2_code.Short_Description as icd_prcdr_2_code_desc,
    int_outpatient.prcdr_dt2, 
    int_outpatient.icd_prcdr_cd3,
    icd_prcdr_3_code.Short_Description as icd_prcdr_3_code_desc,
    int_outpatient.prcdr_dt3, 
    int_outpatient.icd_prcdr_cd4,
    icd_prcdr_4_code.Short_Description as icd_prcdr_4_code_desc,
    int_outpatient.prcdr_dt4, 
    int_outpatient.icd_prcdr_cd5,
    icd_prcdr_5_code.Short_Description as icd_prcdr_5_code_desc, 
    int_outpatient.prcdr_dt5,
    int_outpatient.icd_prcdr_cd6,
    icd_prcdr_6_code.Short_Description as icd_prcdr_6_code_desc,
    int_outpatient.prcdr_dt6,
    int_outpatient.icd_prcdr_cd7,
    icd_prcdr_7_code.Short_Description as icd_prcdr_7_code_desc,
    int_outpatient.prcdr_dt7,
    int_outpatient.icd_prcdr_cd8,
    icd_prcdr_8_code.Short_Description as icd_prcdr_8_code_desc,
    int_outpatient.prcdr_dt8, 
    int_outpatient.icd_prcdr_cd9,
    icd_prcdr_9_code.Short_Description as icd_prcdr_9_code_desc,
    int_outpatient.prcdr_dt9, 
    int_outpatient.icd_prcdr_cd10,
    icd_prcdr_10_code.Short_Description as icd_prcdr_10_code_desc,
    int_outpatient.prcdr_dt10,
    int_outpatient.icd_prcdr_cd11,
    icd_prcdr_11_code.Short_Description as icd_prcdr_11_code_desc,
    int_outpatient.prcdr_dt11,
    int_outpatient.icd_prcdr_cd12,
    icd_prcdr_12_code.Short_Description as icd_prcdr_12_code_desc,
    int_outpatient.prcdr_dt12,
    int_outpatient.icd_prcdr_cd13,
    icd_prcdr_13_code.Short_Description as icd_prcdr_13_code_desc,
    int_outpatient.prcdr_dt13,
    int_outpatient.icd_prcdr_cd14,
    icd_prcdr_14_code.Short_Description as icd_prcdr_14_code_desc,
    int_outpatient.prcdr_dt14,
    int_outpatient.icd_prcdr_cd15,
    icd_prcdr_15_code.Short_Description as icd_prcdr_15_code_desc,
    int_outpatient.prcdr_dt15,
    int_outpatient.icd_prcdr_cd16,
    icd_prcdr_16_code.Short_Description as icd_prcdr_16_code_desc,
    int_outpatient.prcdr_dt16,
    int_outpatient.icd_prcdr_cd17,
    icd_prcdr_17_code.Short_Description as icd_prcdr_17_code_desc,
    int_outpatient.prcdr_dt17,
    int_outpatient.icd_prcdr_cd18,
    icd_prcdr_18_code.Short_Description as icd_prcdr_18_code_desc,
    int_outpatient.prcdr_dt18,
    int_outpatient.icd_prcdr_cd19,
    icd_prcdr_19_code.Short_Description as icd_prcdr_19_code_desc,
    int_outpatient.prcdr_dt19,
    int_outpatient.icd_prcdr_cd20,
    icd_prcdr_20_code.Short_Description as icd_prcdr_20_code_desc,
    int_outpatient.prcdr_dt20,
    int_outpatient.icd_prcdr_cd21,
    icd_prcdr_21_code.Short_Description as icd_prcdr_21_code_desc,
    int_outpatient.prcdr_dt21,
    int_outpatient.icd_prcdr_cd22,
    icd_prcdr_22_code.Short_Description as icd_prcdr_22_code_desc,
    int_outpatient.prcdr_dt22,
    int_outpatient.icd_prcdr_cd23,
    icd_prcdr_23_code.Short_Description as icd_prcdr_23_code_desc,
    int_outpatient.prcdr_dt23,
    int_outpatient.icd_prcdr_cd24,
    icd_prcdr_24_code.Short_Description as icd_prcdr_24_code_desc,
    int_outpatient.prcdr_dt24,
    int_outpatient.icd_prcdr_cd25,
    icd_prcdr_25_code.Short_Description as icd_prcdr_25_code_desc,
    int_outpatient.prcdr_dt25,

    int_outpatient.rsn_visit_cd1,
    int_outpatient.rsn_visit_cd2,
    int_outpatient.rsn_visit_cd3,
    int_outpatient.nch_bene_ptb_ddctbl_amt,
    int_outpatient.nch_bene_ptb_coinsrnc_amt,
    int_outpatient.clm_op_prvdr_pmt_amt,
    int_outpatient.clm_op_bene_pmt_amt,
    int_outpatient.clm_line_num,
    int_outpatient.rev_cntr,
    int_outpatient.rev_cntr_dt,
    int_outpatient.rev_cntr_1st_ansi_cd,
    int_outpatient.rev_cntr_2nd_ansi_cd,
    int_outpatient.rev_cntr_3rd_ansi_cd,
    int_outpatient.rev_cntr_4th_ansi_cd,
    int_outpatient.rev_cntr_apc_hipps_cd,
    int_outpatient.hcpcs_cd,
    hcpcs_code.short_desc as hcpcs_cd_desc,
    int_outpatient.hcpcs_1st_mdfr_cd,
    hcpcs_code_1st_mdfr.short_desc as hcpcs_1st_mdfr_desc,
    int_outpatient.hcpcs_2nd_mdfr_cd,
    hcpcs_code_2nd_mdfr.short_desc as hcpcs_2nd_mdfr_desc,

    int_outpatient.rev_cntr_pmt_mthd_ind_cd,
    int_outpatient.rev_cntr_dscnt_ind_cd,
    int_outpatient.rev_cntr_packg_ind_cd,
    int_outpatient.rev_cntr_otaf_pmt_cd,
    int_outpatient.rev_cntr_ide_ndc_upc_num,
    int_outpatient.rev_cntr_unit_cnt,
    int_outpatient.rev_cntr_rate_amt,
    int_outpatient.rev_cntr_blood_ddctbl_amt,
    int_outpatient.rev_cntr_cash_ddctbl_amt,
    int_outpatient.rev_cntr_coinsrnc_wge_adjstd_c,
    int_outpatient.rev_cntr_rdcd_coinsrnc_amt,
    int_outpatient.rev_cntr_1st_msp_pd_amt,
    int_outpatient.rev_cntr_2nd_msp_pd_amt,
    int_outpatient.rev_cntr_prvdr_pmt_amt,
    int_outpatient.rev_cntr_bene_pmt_amt,
    int_outpatient.rev_cntr_ptnt_rspnsblty_pmt,
    int_outpatient.rev_cntr_pmt_amt_amt,
    int_outpatient.rev_cntr_tot_chrg_amt,
    int_outpatient.rev_cntr_ncvrd_chrg_amt,
    int_outpatient.rev_cntr_stus_ind_cd,
    int_outpatient.rev_cntr_ndc_qty,
    int_outpatient.rev_cntr_ndc_qty_qlfr_cd,
    int_outpatient.rndrng_physn_upin,
    int_outpatient.rndrng_physn_npi

from int_outpatient

left join icd_10_codes as prncpal_dgns_code
on int_outpatient.prncpal_dgns_cd = prncpal_dgns_code.ICD_Code

left join icd_10_codes as icd_dgns_1_code
on int_outpatient.icd_dgns_cd1 = icd_dgns_1_code.ICD_Code
left join icd_10_codes as icd_dgns_2_code
on int_outpatient.icd_dgns_cd2 = icd_dgns_2_code.ICD_Code
left join icd_10_codes as icd_dgns_3_code
on int_outpatient.icd_dgns_cd3 = icd_dgns_3_code.ICD_Code
left join icd_10_codes as icd_dgns_4_code
on int_outpatient.icd_dgns_cd4 = icd_dgns_4_code.ICD_Code
left join icd_10_codes as icd_dgns_5_code
on int_outpatient.icd_dgns_cd5 = icd_dgns_5_code.ICD_Code
left join icd_10_codes as icd_dgns_6_code
on int_outpatient.icd_dgns_cd6 = icd_dgns_6_code.ICD_Code
left join icd_10_codes as icd_dgns_7_code
on int_outpatient.icd_dgns_cd7 = icd_dgns_7_code.ICD_Code
left join icd_10_codes as icd_dgns_8_code
on int_outpatient.icd_dgns_cd8 = icd_dgns_8_code.ICD_Code
left join icd_10_codes as icd_dgns_9_code
on int_outpatient.icd_dgns_cd9 = icd_dgns_9_code.ICD_Code
left join icd_10_codes as icd_dgns_10_code
on int_outpatient.icd_dgns_cd10 = icd_dgns_10_code.ICD_Code
left join icd_10_codes as icd_dgns_11_code
on int_outpatient.icd_dgns_cd11 = icd_dgns_11_code.ICD_Code
left join icd_10_codes as icd_dgns_12_code
on int_outpatient.icd_dgns_cd12 = icd_dgns_12_code.ICD_Code
left join icd_10_codes as icd_dgns_13_code
on int_outpatient.icd_dgns_cd13 = icd_dgns_13_code.ICD_Code
left join icd_10_codes as icd_dgns_14_code
on int_outpatient.icd_dgns_cd14 = icd_dgns_14_code.ICD_Code
left join icd_10_codes as icd_dgns_15_code
on int_outpatient.icd_dgns_cd15 = icd_dgns_15_code.ICD_Code
left join icd_10_codes as icd_dgns_16_code
on int_outpatient.icd_dgns_cd16 = icd_dgns_16_code.ICD_Code
left join icd_10_codes as icd_dgns_17_code
on int_outpatient.icd_dgns_cd17 = icd_dgns_17_code.ICD_Code
left join icd_10_codes as icd_dgns_18_code
on int_outpatient.icd_dgns_cd18 = icd_dgns_18_code.ICD_Code
left join icd_10_codes as icd_dgns_19_code
on int_outpatient.icd_dgns_cd19 = icd_dgns_19_code.ICD_Code
left join icd_10_codes as icd_dgns_20_code
on int_outpatient.icd_dgns_cd20 = icd_dgns_20_code.ICD_Code
left join icd_10_codes as icd_dgns_21_code
on int_outpatient.icd_dgns_cd21 = icd_dgns_21_code.ICD_Code
left join icd_10_codes as icd_dgns_22_code
on int_outpatient.icd_dgns_cd22 = icd_dgns_22_code.ICD_Code
left join icd_10_codes as icd_dgns_23_code
on int_outpatient.icd_dgns_cd23 = icd_dgns_23_code.ICD_Code
left join icd_10_codes as icd_dgns_24_code
on int_outpatient.icd_dgns_cd24 = icd_dgns_24_code.ICD_Code
left join icd_10_codes as icd_dgns_25_code
on int_outpatient.icd_dgns_cd25 = icd_dgns_25_code.ICD_Code


left join icd_10_codes as fst_dgns_e_code
on int_outpatient.fst_dgns_e_cd = fst_dgns_e_code.ICD_Code

left join icd_10_codes as icd_dgns_e_1_code
on int_outpatient.icd_dgns_e_cd1 = icd_dgns_e_1_code.ICD_Code
left join icd_10_codes as icd_dgns_e_2_code
on int_outpatient.icd_dgns_e_cd2 = icd_dgns_e_2_code.ICD_Code
left join icd_10_codes as icd_dgns_e_3_code
on int_outpatient.icd_dgns_e_cd3 = icd_dgns_e_3_code.ICD_Code
left join icd_10_codes as icd_dgns_e_4_code
on int_outpatient.icd_dgns_e_cd4 = icd_dgns_e_4_code.ICD_Code
left join icd_10_codes as icd_dgns_e_5_code
on int_outpatient.icd_dgns_e_cd5 = icd_dgns_e_5_code.ICD_Code
left join icd_10_codes as icd_dgns_e_6_code
on int_outpatient.icd_dgns_e_cd6 = icd_dgns_e_6_code.ICD_Code
left join icd_10_codes as icd_dgns_e_7_code
on int_outpatient.icd_dgns_e_cd7 = icd_dgns_e_7_code.ICD_Code
left join icd_10_codes as icd_dgns_e_8_code
on int_outpatient.icd_dgns_e_cd8 = icd_dgns_e_8_code.ICD_Code
left join icd_10_codes as icd_dgns_e_9_code
on int_outpatient.icd_dgns_e_cd9 = icd_dgns_e_9_code.ICD_Code
left join icd_10_codes as icd_dgns_e_10_code
on int_outpatient.icd_dgns_e_cd10 = icd_dgns_e_10_code.ICD_Code
left join icd_10_codes as icd_dgns_e_11_code
on int_outpatient.icd_dgns_e_cd11 = icd_dgns_e_11_code.ICD_Code
left join icd_10_codes as icd_dgns_e_12_code
on int_outpatient.icd_dgns_e_cd12 = icd_dgns_e_12_code.ICD_Code


left join icd_10_codes as icd_prcdr_1_code
on int_outpatient.icd_prcdr_cd1 = icd_prcdr_1_code.ICD_Code
left join icd_10_codes as icd_prcdr_2_code
on int_outpatient.icd_prcdr_cd2 = icd_prcdr_2_code.ICD_Code
left join icd_10_codes as icd_prcdr_3_code
on int_outpatient.icd_prcdr_cd3 = icd_prcdr_3_code.ICD_Code
left join icd_10_codes as icd_prcdr_4_code
on int_outpatient.icd_prcdr_cd4 = icd_prcdr_4_code.ICD_Code
left join icd_10_codes as icd_prcdr_5_code
on int_outpatient.icd_prcdr_cd5 = icd_prcdr_5_code.ICD_Code
left join icd_10_codes as icd_prcdr_6_code
on int_outpatient.icd_prcdr_cd6 = icd_prcdr_6_code.ICD_Code
left join icd_10_codes as icd_prcdr_7_code
on int_outpatient.icd_prcdr_cd7 = icd_prcdr_7_code.ICD_Code
left join icd_10_codes as icd_prcdr_8_code
on int_outpatient.icd_prcdr_cd8 = icd_prcdr_8_code.ICD_Code
left join icd_10_codes as icd_prcdr_9_code
on int_outpatient.icd_prcdr_cd9 = icd_prcdr_9_code.ICD_Code
left join icd_10_codes as icd_prcdr_10_code
on int_outpatient.icd_prcdr_cd10 = icd_prcdr_10_code.ICD_Code
left join icd_10_codes as icd_prcdr_11_code
on int_outpatient.icd_prcdr_cd11 = icd_prcdr_11_code.ICD_Code
left join icd_10_codes as icd_prcdr_12_code
on int_outpatient.icd_prcdr_cd12 = icd_prcdr_12_code.ICD_Code
left join icd_10_codes as icd_prcdr_13_code
on int_outpatient.icd_prcdr_cd13 = icd_prcdr_13_code.ICD_Code
left join icd_10_codes as icd_prcdr_14_code
on int_outpatient.icd_prcdr_cd14 = icd_prcdr_14_code.ICD_Code
left join icd_10_codes as icd_prcdr_15_code
on int_outpatient.icd_prcdr_cd15 = icd_prcdr_15_code.ICD_Code
left join icd_10_codes as icd_prcdr_16_code
on int_outpatient.icd_prcdr_cd16 = icd_prcdr_16_code.ICD_Code
left join icd_10_codes as icd_prcdr_17_code
on int_outpatient.icd_prcdr_cd17 = icd_prcdr_17_code.ICD_Code
left join icd_10_codes as icd_prcdr_18_code
on int_outpatient.icd_prcdr_cd18 = icd_prcdr_18_code.ICD_Code
left join icd_10_codes as icd_prcdr_19_code
on int_outpatient.icd_prcdr_cd19 = icd_prcdr_19_code.ICD_Code
left join icd_10_codes as icd_prcdr_20_code
on int_outpatient.icd_prcdr_cd20 = icd_prcdr_20_code.ICD_Code
left join icd_10_codes as icd_prcdr_21_code
on int_outpatient.icd_prcdr_cd21 = icd_prcdr_21_code.ICD_Code
left join icd_10_codes as icd_prcdr_22_code
on int_outpatient.icd_prcdr_cd22 = icd_prcdr_22_code.ICD_Code
left join icd_10_codes as icd_prcdr_23_code
on int_outpatient.icd_prcdr_cd23 = icd_prcdr_23_code.ICD_Code
left join icd_10_codes as icd_prcdr_24_code
on int_outpatient.icd_prcdr_cd24 = icd_prcdr_24_code.ICD_Code
left join icd_10_codes as icd_prcdr_25_code
on int_outpatient.icd_prcdr_cd25 = icd_prcdr_25_code.ICD_Code


LEFT JOIN {{ ref('type_of_bill_codes') }} AS type_of_bill_codes 
ON int_outpatient.clm_fac_type_cd = type_of_bill_codes.facilty_type_cd 
AND int_outpatient.clm_srvc_clsfctn_type_cd = type_of_bill_codes.claim_service_type_cd

left join hcpcs_codes as hcpcs_code
  on int_outpatient.hcpcs_cd = coalesce(hcpcs_code.cpt_hcpcs_code, hcpcs_code.betos_code)
  and hcpcs_code.rn = 1

left join hcpcs_codes as hcpcs_code_1st_mdfr
  on int_outpatient.hcpcs_1st_mdfr_cd = coalesce(hcpcs_code_1st_mdfr.cpt_hcpcs_code, hcpcs_code_1st_mdfr.betos_code)
  and hcpcs_code_1st_mdfr.rn = 1

left join hcpcs_codes as hcpcs_code_2nd_mdfr
  on int_outpatient.hcpcs_2nd_mdfr_cd = coalesce(hcpcs_code_2nd_mdfr.cpt_hcpcs_code, hcpcs_code_2nd_mdfr.betos_code)
  and hcpcs_code_2nd_mdfr.rn = 1