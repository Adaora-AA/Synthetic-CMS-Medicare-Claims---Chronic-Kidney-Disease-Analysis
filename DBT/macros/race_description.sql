{#
    This macro returns the description of the line_cms_type_srvc_code 
#}

{% macro race_description(BENE_RACE_CD) -%}

    case {{ dbt.safe_cast(BENE_RACE_CD, api.Column.translate_type("text")) }}   
        when '0' then 'Unknown'
        when '1' then 'White'
        when '2' then 'Black'
        when '3' then 'Other'
        when '4' then 'Asian'
        when '5' then 'Hispanic'
        when '6' then 'North American Native' 
        else 'EMPTY'
    end

{%- endmacro %}