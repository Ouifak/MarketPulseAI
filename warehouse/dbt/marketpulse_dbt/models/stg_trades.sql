{{ config(materialized='view') }}

select *
from {{ source('silver', 'trades') }}