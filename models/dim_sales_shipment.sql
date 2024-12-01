-- dim_sales_shipment
{{
  config(
    materialized='table'
  )
}}

With t_data AS (
SELECT DISTINCT 
    Fulfilment AS fulfillment,
    `ship-postal-code` AS ship_code,
    `ship-country` AS ship_country,
    `Courier Status` AS courier_status,
    Date AS date,
FROM
    {{ source('bronze', 'amazon_sale_report') }}
)
SELECT {{ dbt_utils.generate_surrogate_key([
				'ship_code'
			]) }} as shipment_id, *
FROM t_data