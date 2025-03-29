{#
    This macro returns the description of the line_cms_type_srvc_code 
#}

{% macro ptnt_dschrg_stus_cd_desc(ptnt_dschrg_stus_cd) -%}

    case {{ dbt.safe_cast(ptnt_dschrg_stus_cd, api.Column.translate_type("text")) }}   
        when '0' then 'Unknown Value (but present in data)'
        when '1' then 'Discharged to home/self-care (routine charge)'
        when '2' then 'Discharged/transferred to other short term general hospital for inpatient care'
        when '3' then 'Discharged/transferred to skilled nursing facility (SNF) with Medicare certification in anticipation of covered skilled care — (For hospitals with an approved swing bed arrangement, use Code 61 — swing bed. For reporting discharges/transfers to a non-certified SNF, the hospital must use Code 04 — ICF'
        when '4' then 'Discharged/transferred to intermediate care facility (ICF)'
        when '5' then 'Discharged/transferred to another type of institution for inpatient care (including distinct parts). NOTE: Effective 1/2005,psychiatric hospital or psychiatric distinct part unit of a hospital will no longer be identified by this code. New code is “65”'
        when '6' then 'Discharged/transferred to home care of organized home health service organization'
        when '7' then 'Left against medical advice or discontinued care'
        when '8' then 'Discharged/transferred to home under care of a home IV drug therapy provider. (discontinued eff. 10/1/2005)'
        when '9' then 'Admitted as an inpatient to this hospital (eff. 3/1/1991). In situations where a patient is admitted before midnight of the third day following the day of an outpatient service, the outpatient services are considered inpatient'
        when '20' then 'Expired (patient did not recover)'
        when '21' then 'Discharged/transferred to court/law enforcement'
        when '30' then 'Still patient'
        when '40' then 'Expired at home (hospice claims only)'
        when '41' then 'Expired in a medical facility such as hospital, SNF, ICF, or freestanding hospice. (hospice claims only)'
        when '42' then 'Expired — place unknown (hospice claims only)'
        when '43' then 'Discharged/transferred to a federal hospital (eff. 10/1/2003)'
        when '50' then 'Discharged/transferred to a hospice — home'
        when '51' then 'Discharged/transferred to a hospice — medical facility'
        when '61' then 'Discharged/transferred within this institution to a hospital-based Medicare approved swing bed (eff. 9/2001)'
        when '62' then 'Discharged/transferred to an inpatient rehabilitation facility including distinct parts units of a hospital. (eff. 1/2002)'
        when '63' then 'Discharged/transferred to a longterm care hospital. (eff. 1/2002)'
        when '64' then 'Discharged/transferred to a nursing facility certified under Medicaid but not under Medicare (eff. 10/2002)'
        when '65' then 'Discharged/Transferred to a psychiatric hospital or psychiatric distinct unit of a hospital (these types of hospitals were pulled from patient/discharge status code “05” and given their own code). (eff. 1/2005)'
        when '66' then 'Discharged/transferred to a Critical Access Hospital (CAH) (eff. 1/1/2006)'
        when '69' then 'Discharged/transferred to a designated disaster alternative care site (starting 10/2013; applies only to particular MSDRGs*)'
        when '70' then 'Discharged/transferred to another type of health care institution not defined elsewhere in code list'
        when '71' then 'Discharged/transferred/referred to another institution for outpatient services as specified by the discharge plan of care (eff.9/2001) (discontinued eff.10/1/2005)'
        when '72' then 'Discharged/transferred/referred to this institution for outpatient services as specified by the discharge plan of care (eff.9/2001) (discontinued eff.10/1/2005)'
        when '81' then 'Discharged to home or self-care with a planned acute care hospital inpatient readmission'
        when '82' then 'Discharged/transferred to a short-term general hospital for inpatient care with a planned acute care hospital inpatient readmission'
        when '83' then 'Discharged/transferred to a skilled nursing facility (SNF) with Medicare certification with a planned acute care hospital inpatient readmission'
        when '84' then 'Discharged/transferred to a facility that provides custodial or supportive care with a planned acute care hospital inpatient readmission'
        when '85' then 'Discharged/transferred to a designated cancer center or childrens hospital with a planned acute care hospital inpatient readmission'
        when '86' then 'Discharged/transferred to home under care of organized home health service organization with a planned acute care hospital inpatient readmission'
        when '87' then 'Discharged/transferred to court/law enforcement with a planned acute care hospital inpatient readmission'
        when '88' then 'Discharged/transferred to a federal health care facility with a planned acute care hospital inpatient readmission'
        when '89' then 'Discharged/transferred to a hospital-based Medicare approved swing bed with a planned acute care hospital inpatient readmission'
        when '90' then 'Discharged/transferred to an inpatient rehabilitation facility (IRF) including rehabilitation distinct part units of a hospital with a planned acute care hospital inpatient readmission'
        when '91' then 'Discharged/transferred to a Medicare certified long-term care hospital (LTCH) with a planned acute care hospital inpatient readmission'
        when '92' then 'Discharged/transferred to a nursing facility certified under Medicaid but not certified under Medicare with a planned acute care hospital inpatient readmission'
        when '93' then 'Discharged/transferred to a psychiatric distinct part unit of a hospital with a planned acute care hospital inpatient readmission'
        when '94' then 'Discharged/transferred to a critical access hospital (CAH) with a planned acute care hospital inpatient readmission'
        when '95' then 'Discharged/transferred to another type of health care institution not defined elsewhere in this code list with a planned acute care hospital inpatient readmission'
        else 'EMPTY'
    end

{%- endmacro %}

