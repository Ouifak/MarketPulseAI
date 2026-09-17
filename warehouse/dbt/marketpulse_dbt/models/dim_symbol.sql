{{ config(materialized='table') }}

select distinct symbol
from {{ ref('stg_trades') }}