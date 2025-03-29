{{
    config(
        materialized='view'
    )
}}

with int_inpatient as (
    select *
    from {{ ref('stg_inpatient') }}
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
    int_inpatient.bene_id,
    int_inpatient.clm_id,
    int_inpatient.nch_near_line_rec_ident_cd,
    int_inpatient.nch_clm_type_cd,
    {{ nch_clm_type_description('nch_clm_type_cd','nch_wkly_proc_dt') }} as claim_type_desc,
    int_inpatient.clm_from_dt,
    int_inpatient.clm_thru_dt,
    int_inpatient.nch_wkly_proc_dt,
    int_inpatient.fi_clm_proc_dt,
    int_inpatient.claim_query_code,
    int_inpatient.prvdr_num,
    int_inpatient.clm_fac_type_cd,
    type_of_bill_codes.facility_type as facility_type,
    int_inpatient.clm_srvc_clsfctn_type_cd,
    type_of_bill_codes.claim_service_type as claim_service_type,
    int_inpatient.clm_freq_cd,
    int_inpatient.fi_num,
    int_inpatient.clm_mdcr_non_pmt_rsn_cd,
    int_inpatient.clm_pmt_amt,
    int_inpatient.nch_prmry_pyr_clm_pd_amt,
    int_inpatient.nch_prmry_pyr_cd,
    int_inpatient.fi_clm_actn_cd,
    int_inpatient.prvdr_state_cd,
    {{ state_description('prvdr_state_cd') }} as prvdr_state,
    int_inpatient.org_npi_num,
    int_inpatient.at_physn_upin,
    int_inpatient.at_physn_npi,
    int_inpatient.op_physn_upin,
    int_inpatient.op_physn_npi,
    int_inpatient.ot_physn_upin,
    int_inpatient.ot_physn_npi,
    int_inpatient.clm_mco_pd_sw, 
    int_inpatient.ptnt_dschrg_stus_cd,
    {{ ptnt_dschrg_stus_cd_desc('ptnt_dschrg_stus_cd') }} as patient_discharge_status,
    int_inpatient.clm_pps_ind_cd,
    int_inpatient.clm_tot_chrg_amt,
    int_inpatient.clm_admsn_dt,
    int_inpatient.clm_ip_admsn_type_cd,
    int_inpatient.clm_src_ip_admsn_cd,
    int_inpatient.nch_ptnt_status_ind_cd,
    {{ ptnt_dschrg_stus_ind_desc('nch_ptnt_status_ind_cd') }} as patient_discharge_ind,
    int_inpatient.clm_pass_thru_per_diem_amt,
    int_inpatient.nch_bene_ip_ddctbl_amt,
    int_inpatient.nch_bene_pta_coinsrnc_lblty_am,
    int_inpatient.nch_bene_blood_ddctbl_lblty_am,
    int_inpatient.nch_profnl_cmpnt_chrg_amt,
    int_inpatient.nch_ip_ncvrd_chrg_amt,
    int_inpatient.nch_ip_tot_ddctn_amt,
    int_inpatient.clm_tot_pps_cptl_amt,
    int_inpatient.clm_pps_cptl_fsp_amt,
    int_inpatient.clm_pps_cptl_outlier_amt,
    int_inpatient.clm_pps_cptl_dsprprtnt_shr_amt,
    int_inpatient.clm_pps_cptl_ime_amt,
    int_inpatient.clm_pps_cptl_excptn_amt,
    int_inpatient.clm_pps_old_cptl_hld_hrmls_amt,
    int_inpatient.clm_pps_cptl_drg_wt_num,
    int_inpatient.clm_utlztn_day_cnt,
    int_inpatient.bene_tot_coinsrnc_days_cnt,
    int_inpatient.bene_lrd_used_cnt,
    int_inpatient.clm_non_utlztn_days_cnt,
    int_inpatient.nch_blood_pnts_frnshd_qty,
    int_inpatient.nch_vrfd_ncvrd_stay_from_dt,
    int_inpatient.nch_vrfd_ncvrd_stay_thru_dt,
    int_inpatient.nch_actv_or_cvrd_lvl_care_thru,
    int_inpatient.nch_bene_mdcr_bnfts_exhtd_dt_i,
    int_inpatient.nch_bene_dschrg_dt,
    int_inpatient.clm_drg_cd,
    int_inpatient.clm_drg_outlier_stay_cd,
    int_inpatient.nch_drg_outlier_aprvd_pmt_amt,


    int_inpatient.admtg_dgns_cd,
    admtg_dgns_code.Short_Description as admtg_dgns_code_desc,
    int_inpatient.prncpal_dgns_cd,  
    prncpal_dgns_code.Short_Description as prncpal_dgns_code_desc,
    int_inpatient.icd_dgns_cd1,
    icd_dgns_1_code.Short_Description as icd_dgns_1_code_desc,
    int_inpatient.clm_poa_ind_sw1,
    int_inpatient.icd_dgns_cd2,
    icd_dgns_2_code.Short_Description as icd_dgns_2_code_desc,
    int_inpatient.clm_poa_ind_sw2, 
    int_inpatient.icd_dgns_cd3,
    icd_dgns_3_code.Short_Description as icd_dgns_3_code_desc,
    int_inpatient.clm_poa_ind_sw3, 
    int_inpatient.icd_dgns_cd4,
    icd_dgns_4_code.Short_Description as icd_dgns_4_code_desc,
    int_inpatient.clm_poa_ind_sw4, 
    int_inpatient.icd_dgns_cd5,
    icd_dgns_5_code.Short_Description as icd_dgns_5_code_desc, 
    int_inpatient.clm_poa_ind_sw5,
    int_inpatient.icd_dgns_cd6,
    icd_dgns_6_code.Short_Description as icd_dgns_6_code_desc,
    int_inpatient.clm_poa_ind_sw6,
    int_inpatient.icd_dgns_cd7,
    icd_dgns_7_code.Short_Description as icd_dgns_7_code_desc,
    int_inpatient.clm_poa_ind_sw7,
    int_inpatient.icd_dgns_cd8,
    icd_dgns_8_code.Short_Description as icd_dgns_8_code_desc,
    int_inpatient.clm_poa_ind_sw8, 
    int_inpatient.icd_dgns_cd9,
    icd_dgns_9_code.Short_Description as icd_dgns_9_code_desc,
    int_inpatient.clm_poa_ind_sw9, 
    int_inpatient.icd_dgns_cd10,
    icd_dgns_10_code.Short_Description as icd_dgns_10_code_desc,
    int_inpatient.clm_poa_ind_sw10,
    int_inpatient.icd_dgns_cd11,
    icd_dgns_11_code.Short_Description as icd_dgns_11_code_desc,
    int_inpatient.clm_poa_ind_sw11,
    int_inpatient.icd_dgns_cd12,
    icd_dgns_12_code.Short_Description as icd_dgns_12_code_desc,
    int_inpatient.clm_poa_ind_sw12,
    int_inpatient.icd_dgns_cd13,
    icd_dgns_13_code.Short_Description as icd_dgns_13_code_desc,
    int_inpatient.clm_poa_ind_sw13,
    int_inpatient.icd_dgns_cd14,
    icd_dgns_14_code.Short_Description as icd_dgns_14_code_desc,
    int_inpatient.clm_poa_ind_sw14,
    int_inpatient.icd_dgns_cd15,
    icd_dgns_15_code.Short_Description as icd_dgns_15_code_desc,
    int_inpatient.clm_poa_ind_sw15,
    int_inpatient.icd_dgns_cd16,
    icd_dgns_16_code.Short_Description as icd_dgns_16_code_desc,
    int_inpatient.clm_poa_ind_sw16,
    int_inpatient.icd_dgns_cd17,
    icd_dgns_17_code.Short_Description as icd_dgns_17_code_desc,
    int_inpatient.clm_poa_ind_sw17,
    int_inpatient.icd_dgns_cd18,
    icd_dgns_18_code.Short_Description as icd_dgns_18_code_desc,
    int_inpatient.clm_poa_ind_sw18,
    int_inpatient.icd_dgns_cd19,
    icd_dgns_19_code.Short_Description as icd_dgns_19_code_desc,
    int_inpatient.clm_poa_ind_sw19,
    int_inpatient.icd_dgns_cd20,
    icd_dgns_20_code.Short_Description as icd_dgns_20_code_desc,
    int_inpatient.clm_poa_ind_sw20,
    int_inpatient.icd_dgns_cd21,
    icd_dgns_21_code.Short_Description as icd_dgns_21_code_desc,
    int_inpatient.clm_poa_ind_sw21,
    int_inpatient.icd_dgns_cd22,
    icd_dgns_22_code.Short_Description as icd_dgns_22_code_desc,
    int_inpatient.clm_poa_ind_sw22,
    int_inpatient.icd_dgns_cd23,
    icd_dgns_23_code.Short_Description as icd_dgns_23_code_desc,
    int_inpatient.clm_poa_ind_sw23,
    int_inpatient.icd_dgns_cd24,
    icd_dgns_24_code.Short_Description as icd_dgns_24_code_desc,
    int_inpatient.clm_poa_ind_sw24,
    int_inpatient.icd_dgns_cd25,
    icd_dgns_25_code.Short_Description as icd_dgns_25_code_desc,
    int_inpatient.clm_poa_ind_sw25,

    int_inpatient.fst_dgns_e_cd,
    fst_dgns_e_code.Short_Description as fst_dgns_e_code_desc,
    int_inpatient.icd_dgns_e_cd1,
    icd_dgns_e_1_code.Short_Description as icd_dgns_e_1_code_desc, 
    int_inpatient.clm_e_poa_ind_sw1,
    int_inpatient.icd_dgns_e_cd2,
    icd_dgns_e_2_code.Short_Description as icd_dgns_e_2_code_desc,
    int_inpatient.clm_e_poa_ind_sw2,    
    int_inpatient.icd_dgns_e_cd3,
    icd_dgns_e_3_code.Short_Description as icd_dgns_e_3_code_desc,
    int_inpatient.clm_e_poa_ind_sw3, 
    int_inpatient.icd_dgns_e_cd4,
    icd_dgns_e_4_code.Short_Description as icd_dgns_e_4_code_desc,
    int_inpatient.clm_e_poa_ind_sw4, 
    int_inpatient.icd_dgns_e_cd5,
    icd_dgns_e_5_code.Short_Description as icd_dgns_e_5_code_desc,
    int_inpatient.clm_e_poa_ind_sw5,
    int_inpatient.icd_dgns_e_cd6,
    icd_dgns_e_6_code.Short_Description as icd_dgns_e_6_code_desc,
    int_inpatient.clm_e_poa_ind_sw6,
    int_inpatient.icd_dgns_e_cd7,
    icd_dgns_e_7_code.Short_Description as icd_dgns_e_7_code_desc,
    int_inpatient.clm_e_poa_ind_sw7, 
    int_inpatient.icd_dgns_e_cd8,
    icd_dgns_e_8_code.Short_Description as icd_dgns_e_8_code_desc,
    int_inpatient.clm_e_poa_ind_sw8, 
    int_inpatient.icd_dgns_e_cd9,
    icd_dgns_e_9_code.Short_Description as icd_dgns_e_9_code_desc,
    int_inpatient.clm_e_poa_ind_sw9,
    int_inpatient.icd_dgns_e_cd10,
    icd_dgns_e_10_code.Short_Description as icd_dgns_e_10_code_desc,
    int_inpatient.clm_e_poa_ind_sw10,
    int_inpatient.icd_dgns_e_cd11,
    icd_dgns_e_11_code.Short_Description as icd_dgns_e_11_code_desc,
    int_inpatient.clm_e_poa_ind_sw11, 
    int_inpatient.icd_dgns_e_cd12,
    icd_dgns_e_12_code.Short_Description as icd_dgns_e_12_code_desc,
    int_inpatient.clm_e_poa_ind_sw12,    


    int_inpatient.icd_prcdr_cd1,
    icd_prcdr_1_code.Short_Description as icd_prcdr_1_code_desc,
    int_inpatient.prcdr_dt1,
    int_inpatient.icd_prcdr_cd2,
    icd_prcdr_2_code.Short_Description as icd_prcdr_2_code_desc,
    int_inpatient.prcdr_dt2, 
    int_inpatient.icd_prcdr_cd3,
    icd_prcdr_3_code.Short_Description as icd_prcdr_3_code_desc,
    int_inpatient.prcdr_dt3, 
    int_inpatient.icd_prcdr_cd4,
    icd_prcdr_4_code.Short_Description as icd_prcdr_4_code_desc,
    int_inpatient.prcdr_dt4, 
    int_inpatient.icd_prcdr_cd5,
    icd_prcdr_5_code.Short_Description as icd_prcdr_5_code_desc, 
    int_inpatient.prcdr_dt5,
    int_inpatient.icd_prcdr_cd6,
    icd_prcdr_6_code.Short_Description as icd_prcdr_6_code_desc,
    int_inpatient.prcdr_dt6,
    int_inpatient.icd_prcdr_cd7,
    icd_prcdr_7_code.Short_Description as icd_prcdr_7_code_desc,
    int_inpatient.prcdr_dt7,
    int_inpatient.icd_prcdr_cd8,
    icd_prcdr_8_code.Short_Description as icd_prcdr_8_code_desc,
    int_inpatient.prcdr_dt8, 
    int_inpatient.icd_prcdr_cd9,
    icd_prcdr_9_code.Short_Description as icd_prcdr_9_code_desc,
    int_inpatient.prcdr_dt9, 
    int_inpatient.icd_prcdr_cd10,
    icd_prcdr_10_code.Short_Description as icd_prcdr_10_code_desc,
    int_inpatient.prcdr_dt10,
    int_inpatient.icd_prcdr_cd11,
    icd_prcdr_11_code.Short_Description as icd_prcdr_11_code_desc,
    int_inpatient.prcdr_dt11,
    int_inpatient.icd_prcdr_cd12,
    icd_prcdr_12_code.Short_Description as icd_prcdr_12_code_desc,
    int_inpatient.prcdr_dt12,
    int_inpatient.icd_prcdr_cd13,
    icd_prcdr_13_code.Short_Description as icd_prcdr_13_code_desc,
    int_inpatient.prcdr_dt13,
    int_inpatient.icd_prcdr_cd14,
    icd_prcdr_14_code.Short_Description as icd_prcdr_14_code_desc,
    int_inpatient.prcdr_dt14,
    int_inpatient.icd_prcdr_cd15,
    icd_prcdr_15_code.Short_Description as icd_prcdr_15_code_desc,
    int_inpatient.prcdr_dt15,
    int_inpatient.icd_prcdr_cd16,
    icd_prcdr_16_code.Short_Description as icd_prcdr_16_code_desc,
    int_inpatient.prcdr_dt16,
    int_inpatient.icd_prcdr_cd17,
    icd_prcdr_17_code.Short_Description as icd_prcdr_17_code_desc,
    int_inpatient.prcdr_dt17,
    int_inpatient.icd_prcdr_cd18,
    icd_prcdr_18_code.Short_Description as icd_prcdr_18_code_desc,
    int_inpatient.prcdr_dt18,
    int_inpatient.icd_prcdr_cd19,
    icd_prcdr_19_code.Short_Description as icd_prcdr_19_code_desc,
    int_inpatient.prcdr_dt19,
    int_inpatient.icd_prcdr_cd20,
    icd_prcdr_20_code.Short_Description as icd_prcdr_20_code_desc,
    int_inpatient.prcdr_dt20,
    int_inpatient.icd_prcdr_cd21,
    icd_prcdr_21_code.Short_Description as icd_prcdr_21_code_desc,
    int_inpatient.prcdr_dt21,
    int_inpatient.icd_prcdr_cd22,
    icd_prcdr_22_code.Short_Description as icd_prcdr_22_code_desc,
    int_inpatient.prcdr_dt22,
    int_inpatient.icd_prcdr_cd23,
    icd_prcdr_23_code.Short_Description as icd_prcdr_23_code_desc,
    int_inpatient.prcdr_dt23,
    int_inpatient.icd_prcdr_cd24,
    icd_prcdr_24_code.Short_Description as icd_prcdr_24_code_desc,
    int_inpatient.prcdr_dt24,
    int_inpatient.icd_prcdr_cd25,
    icd_prcdr_25_code.Short_Description as icd_prcdr_25_code_desc,
    int_inpatient.prcdr_dt25,
    int_inpatient.ime_op_clm_val_amt,
    int_inpatient.dsh_op_clm_val_amt,
    int_inpatient.clm_uncompd_care_pmt_amt,
    int_inpatient.clm_line_num,
    int_inpatient.rev_cntr,
    int_inpatient.hcpcs_cd,
    hcpcs_code.short_desc as hcpcs_cd_desc,
    int_inpatient.rev_cntr_ddctbl_coinsrnc_cd

from int_inpatient

left join icd_10_codes as admtg_dgns_code
on int_inpatient.admtg_dgns_cd = admtg_dgns_code.ICD_Code

left join icd_10_codes as prncpal_dgns_code
on int_inpatient.prncpal_dgns_cd = prncpal_dgns_code.ICD_Code

left join icd_10_codes as icd_dgns_1_code
on int_inpatient.icd_dgns_cd1 = icd_dgns_1_code.ICD_Code
left join icd_10_codes as icd_dgns_2_code
on int_inpatient.icd_dgns_cd2 = icd_dgns_2_code.ICD_Code
left join icd_10_codes as icd_dgns_3_code
on int_inpatient.icd_dgns_cd3 = icd_dgns_3_code.ICD_Code
left join icd_10_codes as icd_dgns_4_code
on int_inpatient.icd_dgns_cd4 = icd_dgns_4_code.ICD_Code
left join icd_10_codes as icd_dgns_5_code
on int_inpatient.icd_dgns_cd5 = icd_dgns_5_code.ICD_Code
left join icd_10_codes as icd_dgns_6_code
on int_inpatient.icd_dgns_cd6 = icd_dgns_6_code.ICD_Code
left join icd_10_codes as icd_dgns_7_code
on int_inpatient.icd_dgns_cd7 = icd_dgns_7_code.ICD_Code
left join icd_10_codes as icd_dgns_8_code
on int_inpatient.icd_dgns_cd8 = icd_dgns_8_code.ICD_Code
left join icd_10_codes as icd_dgns_9_code
on int_inpatient.icd_dgns_cd9 = icd_dgns_9_code.ICD_Code
left join icd_10_codes as icd_dgns_10_code
on int_inpatient.icd_dgns_cd10 = icd_dgns_10_code.ICD_Code
left join icd_10_codes as icd_dgns_11_code
on int_inpatient.icd_dgns_cd11 = icd_dgns_11_code.ICD_Code
left join icd_10_codes as icd_dgns_12_code
on int_inpatient.icd_dgns_cd12 = icd_dgns_12_code.ICD_Code
left join icd_10_codes as icd_dgns_13_code
on int_inpatient.icd_dgns_cd13 = icd_dgns_13_code.ICD_Code
left join icd_10_codes as icd_dgns_14_code
on int_inpatient.icd_dgns_cd14 = icd_dgns_14_code.ICD_Code
left join icd_10_codes as icd_dgns_15_code
on int_inpatient.icd_dgns_cd15 = icd_dgns_15_code.ICD_Code
left join icd_10_codes as icd_dgns_16_code
on int_inpatient.icd_dgns_cd16 = icd_dgns_16_code.ICD_Code
left join icd_10_codes as icd_dgns_17_code
on int_inpatient.icd_dgns_cd17 = icd_dgns_17_code.ICD_Code
left join icd_10_codes as icd_dgns_18_code
on int_inpatient.icd_dgns_cd18 = icd_dgns_18_code.ICD_Code
left join icd_10_codes as icd_dgns_19_code
on int_inpatient.icd_dgns_cd19 = icd_dgns_19_code.ICD_Code
left join icd_10_codes as icd_dgns_20_code
on int_inpatient.icd_dgns_cd20 = icd_dgns_20_code.ICD_Code
left join icd_10_codes as icd_dgns_21_code
on int_inpatient.icd_dgns_cd21 = icd_dgns_21_code.ICD_Code
left join icd_10_codes as icd_dgns_22_code
on int_inpatient.icd_dgns_cd22 = icd_dgns_22_code.ICD_Code
left join icd_10_codes as icd_dgns_23_code
on int_inpatient.icd_dgns_cd23 = icd_dgns_23_code.ICD_Code
left join icd_10_codes as icd_dgns_24_code
on int_inpatient.icd_dgns_cd24 = icd_dgns_24_code.ICD_Code
left join icd_10_codes as icd_dgns_25_code
on int_inpatient.icd_dgns_cd25 = icd_dgns_25_code.ICD_Code


left join icd_10_codes as fst_dgns_e_code
on int_inpatient.fst_dgns_e_cd = fst_dgns_e_code.ICD_Code

left join icd_10_codes as icd_dgns_e_1_code
on int_inpatient.icd_dgns_e_cd1 = icd_dgns_e_1_code.ICD_Code
left join icd_10_codes as icd_dgns_e_2_code
on int_inpatient.icd_dgns_e_cd2 = icd_dgns_e_2_code.ICD_Code
left join icd_10_codes as icd_dgns_e_3_code
on int_inpatient.icd_dgns_e_cd3 = icd_dgns_e_3_code.ICD_Code
left join icd_10_codes as icd_dgns_e_4_code
on int_inpatient.icd_dgns_e_cd4 = icd_dgns_e_4_code.ICD_Code
left join icd_10_codes as icd_dgns_e_5_code
on int_inpatient.icd_dgns_e_cd5 = icd_dgns_e_5_code.ICD_Code
left join icd_10_codes as icd_dgns_e_6_code
on int_inpatient.icd_dgns_e_cd6 = icd_dgns_e_6_code.ICD_Code
left join icd_10_codes as icd_dgns_e_7_code
on int_inpatient.icd_dgns_e_cd7 = icd_dgns_e_7_code.ICD_Code
left join icd_10_codes as icd_dgns_e_8_code
on int_inpatient.icd_dgns_e_cd8 = icd_dgns_e_8_code.ICD_Code
left join icd_10_codes as icd_dgns_e_9_code
on int_inpatient.icd_dgns_e_cd9 = icd_dgns_e_9_code.ICD_Code
left join icd_10_codes as icd_dgns_e_10_code
on int_inpatient.icd_dgns_e_cd10 = icd_dgns_e_10_code.ICD_Code
left join icd_10_codes as icd_dgns_e_11_code
on int_inpatient.icd_dgns_e_cd11 = icd_dgns_e_11_code.ICD_Code
left join icd_10_codes as icd_dgns_e_12_code
on int_inpatient.icd_dgns_e_cd12 = icd_dgns_e_12_code.ICD_Code


left join icd_10_codes as icd_prcdr_1_code
on int_inpatient.icd_prcdr_cd1 = icd_prcdr_1_code.ICD_Code
left join icd_10_codes as icd_prcdr_2_code
on int_inpatient.icd_prcdr_cd2 = icd_prcdr_2_code.ICD_Code
left join icd_10_codes as icd_prcdr_3_code
on int_inpatient.icd_prcdr_cd3 = icd_prcdr_3_code.ICD_Code
left join icd_10_codes as icd_prcdr_4_code
on int_inpatient.icd_prcdr_cd4 = icd_prcdr_4_code.ICD_Code
left join icd_10_codes as icd_prcdr_5_code
on int_inpatient.icd_prcdr_cd5 = icd_prcdr_5_code.ICD_Code
left join icd_10_codes as icd_prcdr_6_code
on int_inpatient.icd_prcdr_cd6 = icd_prcdr_6_code.ICD_Code
left join icd_10_codes as icd_prcdr_7_code
on int_inpatient.icd_prcdr_cd7 = icd_prcdr_7_code.ICD_Code
left join icd_10_codes as icd_prcdr_8_code
on int_inpatient.icd_prcdr_cd8 = icd_prcdr_8_code.ICD_Code
left join icd_10_codes as icd_prcdr_9_code
on int_inpatient.icd_prcdr_cd9 = icd_prcdr_9_code.ICD_Code
left join icd_10_codes as icd_prcdr_10_code
on int_inpatient.icd_prcdr_cd10 = icd_prcdr_10_code.ICD_Code
left join icd_10_codes as icd_prcdr_11_code
on int_inpatient.icd_prcdr_cd11 = icd_prcdr_11_code.ICD_Code
left join icd_10_codes as icd_prcdr_12_code
on int_inpatient.icd_prcdr_cd12 = icd_prcdr_12_code.ICD_Code
left join icd_10_codes as icd_prcdr_13_code
on int_inpatient.icd_prcdr_cd13 = icd_prcdr_13_code.ICD_Code
left join icd_10_codes as icd_prcdr_14_code
on int_inpatient.icd_prcdr_cd14 = icd_prcdr_14_code.ICD_Code
left join icd_10_codes as icd_prcdr_15_code
on int_inpatient.icd_prcdr_cd15 = icd_prcdr_15_code.ICD_Code
left join icd_10_codes as icd_prcdr_16_code
on int_inpatient.icd_prcdr_cd16 = icd_prcdr_16_code.ICD_Code
left join icd_10_codes as icd_prcdr_17_code
on int_inpatient.icd_prcdr_cd17 = icd_prcdr_17_code.ICD_Code
left join icd_10_codes as icd_prcdr_18_code
on int_inpatient.icd_prcdr_cd18 = icd_prcdr_18_code.ICD_Code
left join icd_10_codes as icd_prcdr_19_code
on int_inpatient.icd_prcdr_cd19 = icd_prcdr_19_code.ICD_Code
left join icd_10_codes as icd_prcdr_20_code
on int_inpatient.icd_prcdr_cd20 = icd_prcdr_20_code.ICD_Code
left join icd_10_codes as icd_prcdr_21_code
on int_inpatient.icd_prcdr_cd21 = icd_prcdr_21_code.ICD_Code
left join icd_10_codes as icd_prcdr_22_code
on int_inpatient.icd_prcdr_cd22 = icd_prcdr_22_code.ICD_Code
left join icd_10_codes as icd_prcdr_23_code
on int_inpatient.icd_prcdr_cd23 = icd_prcdr_23_code.ICD_Code
left join icd_10_codes as icd_prcdr_24_code
on int_inpatient.icd_prcdr_cd24 = icd_prcdr_24_code.ICD_Code
left join icd_10_codes as icd_prcdr_25_code
on int_inpatient.icd_prcdr_cd25 = icd_prcdr_25_code.ICD_Code

LEFT JOIN {{ ref('type_of_bill_codes') }} AS type_of_bill_codes 
ON int_inpatient.clm_fac_type_cd = type_of_bill_codes.facilty_type_cd 
AND int_inpatient.clm_srvc_clsfctn_type_cd = type_of_bill_codes.claim_service_type_cd

left join hcpcs_codes as hcpcs_code
on int_inpatient.hcpcs_cd = coalesce(hcpcs_code.cpt_hcpcs_code, hcpcs_code.betos_code)
and hcpcs_code.rn = 1