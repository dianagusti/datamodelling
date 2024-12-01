-- dim_sales_channel
{{
  config(
    materialized='table'
  )
}}

With t_data AS (
SELECT DISTINCT 
  `Sales Channel ` AS sales_channel,
  B2B AS b2b,
FROM
    {{ source('bronze', 'amazon_sale_report') }}
)

SELECT {{ dbt_utils.generate_surrogate_key([
				'sales_channel'
			]) }} as channel_id, *
FROM t_data