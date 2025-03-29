{#
    This macro returns the description of the line_cms_type_srvc_code 
#}

{% macro ptnt_dschrg_stus_ind_desc(nch_ptnt_status_ind_cd) -%}

    case {{ dbt.safe_cast(nch_ptnt_status_ind_cd, api.Column.translate_type("text")) }}   
        when 'A' then 'Discharged'
        when 'B' then 'Died'
        when 'C' then 'Still a patient'
        else 'EMPTY'
    end

{%- endmacro %}