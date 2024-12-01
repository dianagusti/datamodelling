-- fact_sales
{{
  config(
    materialized='table'
  )
}}

SELECT 
  `Order ID` AS order_id, 
  Date AS date,
  Status AS status,
  {{ dbt_utils.generate_surrogate_key([
      'SKU'
  ]) }} AS product_id,
  {{ dbt_utils.generate_surrogate_key([
      'Fulfilment', 
      '`fulfilled-by`'
  ]) }} AS fulfillment_id,
  {{ dbt_utils.generate_surrogate_key([
      '`promotion-ids`'
  ]) }} AS promotion_id,
  SUM(qty) AS qty,
  COALESCE(SUM(amount), 0) AS amount
FROM
    {{ source('bronze', 'amazon_sale_report') }}
GROUP BY 
  `Order ID`, 
  Date, 
  Status, 
  {{ dbt_utils.generate_surrogate_key(['SKU']) }},
  {{ dbt_utils.generate_surrogate_key(['Fulfilment', '`fulfilled-by`']) }},
  {{ dbt_utils.generate_surrogate_key(['`promotion-ids`']) }}

