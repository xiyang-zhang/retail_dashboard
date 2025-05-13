/*
Name: dws_campaign_cost_sale_prd_d_inc
Description: Combine cost and sales for each product and campaign result
Data modeling reference: https://docs.google.com/spreadsheets/d/1FlbiZEBue2SCAUo1WOkdtX5WbEDyDrZ9B2uiZUSiRio/edit?gid=286240572#gid=286240572
Target Table: dws_campaign_cost_sales_prd_d_inc
Source Table: dws_sale_customer_prd_d_inc, dws_campaign_cost_prd_d_inc, dim_product_full
Created by: alvinxyzhang
Created Date: 2025-04-29
Version: v1.0
*/

DECLARE date_ DATE DEFAULT DATE(2025, 3, 20); # any date of the campaign execution #  CURRENT_DATE()

WITH sale_tbl AS (
  SELECT `date`
      , product_id
      , purchase_type
      , product_name
      , category
      , brand
      , SUM(number_of_items) AS number_of_items
      , AVG(retail_price) AS retail_price
      , AVG(retail_cost) AS retail_cost
      , SUM(total_price) AS total_price
      , SUM(total_cost) AS total_cost
  FROM `positive-karma-457703-i3.retail_dashboard.dws_sale_customer_prd_d_inc`
  WHERE `date` = date_
  GROUP BY `date`
      , product_id
      , purchase_type
      , product_name
      , category
      , brand
), campaign_tbl AS (
  SELECT `date`
      , product_id
      , campaign_id
      , campaign_name
      , campaign_type
      , start_date
      , end_date
      , estimated_budget
      , approved_budget
      , real_spent_budget
      , discount
      , estimated_sales_increase
      , count_product
      , SUM(cost_in_store) AS cost_in_store
      , SUM(cost_flyer) AS cost_flyer
      , SUM(cost_community) AS cost_community
      , SUM(cost_loyalty) AS cost_loyalty
      , SUM(cost_push) AS cost_push
      , SUM(cost_ad_total) AS cost_ad_total
      , SUM(cost_ad_search) AS cost_ad_search
      , SUM(cost_ad_display) AS cost_ad_display
      , SUM(cost_ad_event) AS cost_ad_event
      , SUM(cost_ad_retargeting) AS cost_ad_retargeting
      , SUM(cost_ad_email) AS cost_ad_email
      , SUM(impressions_ad_total) AS impressions_ad_total
      , SUM(impressions_ad_search) AS impressions_ad_search
      , SUM(impressions_ad_display) AS impressions_ad_display
      , SUM(impressions_ad_event) AS impressions_ad_event
      , SUM(impressions_ad_retargeting) AS impressions_ad_retargeting
      , SUM(impressions_ad_email) AS impressions_ad_email
      , SUM(clicks_ad_total) AS clicks_ad_total
      , SUM(clicks_ad_search) AS clicks_ad_search
      , SUM(clicks_ad_display) AS clicks_ad_display
      , SUM(clicks_ad_event) AS clicks_ad_event
      , SUM(clicks_ad_retargeting) AS clicks_ad_retargeting
      , SUM(clicks_ad_email) AS clicks_ad_email
      , SUM(conversions_ad_total) AS conversions_ad_total
      , SUM(conversions_ad_search) AS conversions_ad_search
      , SUM(conversions_ad_display) AS conversions_ad_display
      , SUM(conversions_ad_event) AS conversions_ad_event
      , SUM(conversions_ad_retargeting) AS conversions_ad_retargeting
      , SUM(conversions_ad_email) AS conversions_ad_email
  FROM `positive-karma-457703-i3.retail_dashboard.dws_campaign_cost_prd_d_inc`
  WHERE `date` = date_
  GROUP BY `date`
        , product_id
        , campaign_id
        , campaign_name
        , campaign_type
        , start_date
        , end_date
        , estimated_budget
        , approved_budget
        , real_spent_budget
        , discount
        , estimated_sales_increase
        , count_product
  ORDER BY product_id
)

SELECT date_ AS `date`
      , prd_tbl.product_id AS product_id
      , purchase_type
      , prd_tbl.product_name AS product_name
      , prd_tbl.category AS category
      , prd_tbl.brand AS brand
      , coalesce(number_of_items, 0) AS number_of_items
      , coalesce(sale_tbl.retail_price, prd_tbl.retail_price) AS retail_price
      , coalesce(sale_tbl.retail_cost, prd_tbl.cost) AS retail_cost
      , coalesce(total_price, 0) AS total_price 
      , coalesce(total_cost, 0) AS total_cost
      , campaign_id
      , campaign_name
      , campaign_type
      , start_date
      , end_date
      , estimated_budget
      , approved_budget
      , real_spent_budget
      , discount
      , estimated_sales_increase
      , count_product
      , cost_in_store
      , cost_flyer
      , cost_community
      , cost_loyalty
      , cost_push
      , cost_ad_total
      , cost_ad_search
      , cost_ad_display
      , cost_ad_event
      , cost_ad_retargeting
      , cost_ad_email
      , impressions_ad_total
      , impressions_ad_search
      , impressions_ad_display
      , impressions_ad_event
      , impressions_ad_retargeting
      , impressions_ad_email
      , clicks_ad_total
      , clicks_ad_search
      , clicks_ad_display
      , clicks_ad_event
      , clicks_ad_retargeting
      , clicks_ad_email
      , conversions_ad_total
      , conversions_ad_search
      , conversions_ad_display
      , conversions_ad_event
      , conversions_ad_retargeting
      , conversions_ad_email
      , coalesce(sale_tbl.retail_price, prd_tbl.retail_price) * (1 - discount) AS retail_discount_price
      , coalesce(total_price, 0) * (1 - discount) AS total_discount_price
FROM `positive-karma-457703-i3.retail_dashboard.dim_product_full` AS prd_tbl
LEFT JOIN sale_tbl 
  ON sale_tbl.product_id = prd_tbl.product_id
LEFT JOIN campaign_tbl
  ON campaign_tbl.product_id = prd_tbl.product_id
ORDER BY prd_tbl.product_id ASC