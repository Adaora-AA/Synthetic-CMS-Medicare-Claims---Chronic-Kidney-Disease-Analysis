{#
    This macro returns the description of the nch_claim_type.
#}

{% macro nch_clm_type_description(nch_clm_type_cd, nch_wkly_proc_dt) -%}

    case 
        when {{ dbt.safe_cast(nch_clm_type_cd, api.Column.translate_type("text")) }} = '81' 
             and {{ dbt.safe_cast(nch_wkly_proc_dt, api.Column.translate_type("text")) }} = '2023-01-27' 
        then 'DMERC; DMEPOS claim'
        
        when {{ dbt.safe_cast(nch_clm_type_cd, api.Column.translate_type("text")) }} = '71' 
             and {{ dbt.safe_cast(nch_wkly_proc_dt, api.Column.translate_type("text")) }} = '2023-01-27' 
        then 'Local carrier DMEPOS claim'
        
        else case {{ dbt.safe_cast(nch_clm_type_cd, api.Column.translate_type("text")) }}   
            when '10' then 'Home health agency (HHA) claim'
            when '20' then 'Non swing bed skilled nursing facility (SNF) claim'
            when '30' then 'Swing bed SNF claim'
            when '40' then 'Hospital outpatient claim'
            when '50' then 'Hospice claim'
            when '60' then 'Inpatient claim'
            when '71' then 'Local carrier non-durable medical equipment, prosthetics, orthotics, and supplies (DMEPOS) claim'
            when '72' then 'Local carrier DMEPOS claim'
            when '81' then 'Durable medical equipment regional carrier (DMERC); non-DMEPOS claim'
            when '82' then 'DMERC; DMEPOS claim'
            else 'EMPTY'
        end
    end

{%- endmacro %}
