/*
Name: insert_dws_sale_customer_prd_d_inc
Description: insert data to dws_sale_customer_prd_d_inc. Complete sales table for further understanding of product_id, customer_id, stores.
Data modeling reference: https://docs.google.com/spreadsheets/d/1FlbiZEBue2SCAUo1WOkdtX5WbEDyDrZ9B2uiZUSiRio/edit?gid=596791655#gid=596791655
Target Table: dws_sale_customer_prd_d_inc
Source Table: dwd_sale_d_inc, dim_product_full, dim_store_full, dim_city_full, dim_customer_full
Created by: alvinxyzhang
Created Date: 2025-04-29
Version: v1.0
*/

DECLARE date_ DATE DEFAULT DATE(2024, 2, 28); # CURRENT_DATE()


SELECT `date`
    , sale_tbl.product_id
    , purchase_type
    , product_name
    , category
    , brand
    , order_id
    , SUM(number_of_items) number_of_items
    , AVG(retail_price) retail_price
    , AVG(cost) retail_cost
    , SUM(number_of_items * retail_price) total_price
    , SUM(number_of_items * cost) total_cost
    , sale_tbl.customer_id
    , home_address
    , customer_tbl.longitude AS customer_long
    , customer_tbl.latitude AS customer_lat 
    , email
    , age
    , gender
    , account_created_date
    , is_vip_membership
    , family_size
    , occupation
    , annual_salary_estimate
    , ad_group
    , sale_tbl.store_id
    , store_name
    , store_tbl.address AS store_address
    , store_tbl.city
    , province
    , store_tbl.lat AS store_lat
    , store_tbl.long AS store_long
    , store_tbl.population AS city_population
    , city_tbl.lat AS city_lat
    , city_tbl.long AS city_long
FROM 
(
  SELECT sale_date AS date
      , product_id
      , customer_id
      , store_id
      , order_id
      , purchase_type
      , SUM(number_of_items) number_of_items
  FROM `positive-karma-457703-i3.retail_dashboard.dwd_sale_d_inc`
  WHERE sale_date = date_
  GROUP BY sale_date
      , product_id
      , customer_id
      , store_id
      , order_id
      , purchase_type
) AS sale_tbl
LEFT JOIN `positive-karma-457703-i3.retail_dashboard.dim_product_full` AS prd_tbl
  ON sale_tbl.product_id = prd_tbl.product_id
LEFT JOIN `positive-karma-457703-i3.retail_dashboard.dim_customer_full` AS customer_tbl
  ON sale_tbl.customer_id = customer_tbl.customer_id
LEFT JOIN `positive-karma-457703-i3.retail_dashboard.dim_store_full` AS store_tbl
  ON sale_tbl.store_id = store_tbl.store_id
LEFT JOIN `positive-karma-457703-i3.retail_dashboard.dim_city_full` AS city_tbl
  ON store_tbl.city = city_tbl.city
GROUP BY `date`
      , sale_tbl.product_id
      , purchase_type
      , product_name
      , category
      , brand
      , order_id
      , sale_tbl.customer_id
      , home_address
      , customer_tbl.longitude
      , customer_tbl.latitude
      , email
      , age
      , gender
      , account_created_date
      , is_vip_membership
      , family_size
      , occupation
      , annual_salary_estimate
      , ad_group
      , sale_tbl.store_id
      , store_name
      , store_tbl.address
      , store_tbl.city
      , province
      , store_tbl.lat
      , store_tbl.long
      , store_tbl.population
      , city_tbl.lat
      , city_tbl.long

