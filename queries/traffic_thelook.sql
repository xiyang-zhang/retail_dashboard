WITH user_tbl as (
    SELECT date(created_at) traffic_day
        , id as user_id
        , traffic_source
        , age
        , gender
        , country
        , state
        , city
        , user_geom
    FROM `bigquery-public-data.thelook_ecommerce.users` AS u
    WHERE date(created_at) >= '2024-01-01' # @user_record_date_start
      AND date(created_at) <= '2025-01-31' # @user_record_date_end
    ORDER BY user_record_order DESC
),
event_tbl AS (
# attribution: uri /product/xxxx corresponding to the product_id
    SELECT date(created_at) event_day
        , user_id
        , traffic_source
        , event_type
        , uri
        , SPLIT(uri, '/')[2] product_id
    FROM `bigquery-public-data.thelook_ecommerce.events` AS e
    WHERE date(created_at) >= '2025-01-01' # @event_date_start
      AND date(created_at) <= '2025-01-31' # @event_date_end
      AND event_type = 'product'
), 
WITH order_tbl as (
SELECT date(created_at) order_day
    , order_id
    , user_id
    , status
    , num_of_item
FROM `bigquery-public-data.thelook_ecommerce.orders` AS o
WHERE date(created_at) >= '2025-01-01' # @order_date_start
  AND date(created_at) <= '2025-01-31' # @order_date_end
),
order_item_tbl as (
SELECT date(created_at) order_item_day
     , order_id
     , user_id
     , product_id
     , sale_price
     , status
FROM `bigquery-public-data.thelook_ecommerce.order_items` AS o
WHERE date(created_at) >= '2025-01-01'
  AND date(created_at) <= '2025-01-31'
  AND status = 'Complete' # only look at completed order_items
)
SELECT order_day
     , product_id
     , order_tbl.user_id
     , SUM(sale_price) sale_price
FROM order_tbl 
LEFT JOIN order_item_tbl
 ON order_tbl.order_id = order_item_tbl.order_id
AND order_tbl.user_id = order_item_tbl.user_id
GROUP BY order_day
     , product_id
     , order_tbl.user_id
) 
ORDER BY 1

SELECT *
FROM `bigquery-public-data.thelook_ecommerce.events` AS e
WHERE user_id = 731
ORDER BY created_at ASC

SELECT *
FROM `bigquery-public-data.thelook_ecommerce.order_items` 
WHERE product_id = 23615
  AND user_id = 27718

SELECT * 
FROM `bigquery-public-data.thelook_ecommerce.orders` 
WHERE user_id = 27718