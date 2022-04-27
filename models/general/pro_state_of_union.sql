{{ config(
    materialized = "table"
) }}
Select *

FROM {{ ref('pro_survey') }}