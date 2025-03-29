{#
    This macro returns the description of the line_cms_type_srvc_code 
#}

{% macro line_cms_type_srvc_cd_description(line_cms_type_srvc_cd) -%}

    case {{ dbt.safe_cast(line_cms_type_srvc_cd, api.Column.translate_type("text")) }}   
        when '1' then 'Medical care'
        when '2' then 'Surgery'
        when '3' then 'Consultation'
        when '4' then 'Diagnostic radiology'
        when '5' then 'Diagnostic laboratory'
        when '6' then 'Therapeutic radiology'
        when '7' then 'Anesthesia'
        when '8' then 'Assistant at surgery'
        when '9' then 'Other medical items or services'
        when '0' then 'Whole blood'
        when 'A' then 'Used durable medical equipment (DME)'
        when 'D' then 'Ambulance'
        when 'E' then 'Enteral/parenteral nutrients/supplies'
        when 'F' then 'Ambulatory surgical center (facility usage for surgical services)'
        when 'G' then 'Immunosuppressive drugs'
        when 'J' then 'Diabetic shoes'
        when 'K' then 'Hearing items and services'
        when 'L' then 'ESRD supplies'
        when 'M' then 'Monthly capitation payment for dialysis'
        when 'N' then 'Kidney donor'
        when 'P' then 'Lump sum purchase of DME,prosthetics orthotics'
        when 'Q' then 'Vision items or services'
        when 'R' then 'Rental of DME'
        when 'S' then 'Surgical dressings or other medical supplies'
        when 'T' then 'Outpatient mental health limitation'
        when 'U' then 'Occupational therapy'
        when 'V' then 'Pneumococcal/flu vaccine'
        when 'W' then 'Physical therapy'
        else 'EMPTY'
    end

{%- endmacro %}