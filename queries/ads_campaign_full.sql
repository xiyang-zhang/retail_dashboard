/*
Name: ads_campaign_full
Description: all campaign summary, including in store and online.
Data modeling reference: https://docs.google.com/spreadsheets/d/1FlbiZEBue2SCAUo1WOkdtX5WbEDyDrZ9B2uiZUSiRio/edit?gid=2043216767#gid=2043216767
Target Table: ads_campaign_full
Source Table: dws_campaign_cost_sales_prd_d_inc
Created by: alvinxyzhang
Created Date: 2025-05-01
Version: v1.0
*/

-- SELECT *
-- FROM `positive-karma-457703-i3.retail_dashboard.ads_campaign_full`

DECLARE campaign_id_ DEFAULT 1;
DECLARE post_campaign_start_date_, post_campaign_end_date_, baseline_start_date_, baseline_end_date_ DATE;
DECLARE window_len_ DEFAULT 7; # window length = 1 week

SET (campaign_id_, post_campaign_start_date_, post_campaign_end_date_, baseline_start_date_, baseline_end_date_) = (
  SELECT AS STRUCT campaign_id # assupmtion: only one campaign happens during one period
       , start_date AS post_campaign_start_date
       , DATE_ADD(end_date, INTERVAL window_len_ - 1 DAY) AS post_campaign_end_date # including today + the following 6 days = 1 week
       , DATE_SUB(start_date, INTERVAL campaign_length + window_len_ DAY) AS baseline_start_date
       , DATE_SUB(start_date, INTERVAL 1 DAY) AS baseline_end_date
  FROM (
    SELECT DISTINCT campaign_id, start_date, end_date, DATE_DIFF(end_date, start_date, DAY) + 1 campaign_length
    FROM `positive-karma-457703-i3.retail_dashboard.dwd_campaign_d_inc`
    WHERE campaign_id = campaign_id_
  ) AS t0
); # find the corresponding campaign date and window for baseline and post-campaign

WITH post_campaign_tbl AS (
  SELECT product_id
        , SUM(number_of_items) number_of_items_sum
        , AVG(retail_price) retail_price
        , AVG(retail_cost) retail_cost
        , SUM(total_price) total_price_sum
        , SUM(total_cost) total_cost_sum
        , AVG(retail_discount_price) retail_discount_price
        , SUM(total_discount_price) total_discount_price_sum
  FROM `positive-karma-457703-i3.retail_dashboard.dws_campaign_cost_sale_prd_d_inc`
  WHERE `date` >= post_campaign_start_date_ and `date` <= post_campaign_end_date_
    AND purchase_type = 'instore'
    AND (campaign_id = campaign_id_ OR campaign_id IS NULL) # exclude other campaign_id
  GROUP BY product_id
)
, baseline_tbl AS (
  SELECT product_id
      , SUM(number_of_items) number_of_items_sum
      , AVG(retail_price) retail_price
      , AVG(retail_cost) retail_cost
      , SUM(total_price) total_price_sum
      , SUM(total_cost) total_cost_sum
      , AVG(retail_discount_price) retail_discount_price
      , SUM(total_discount_price) total_discount_price_sum
  FROM `positive-karma-457703-i3.retail_dashboard.dws_campaign_cost_sale_prd_d_inc`
  WHERE `date` >= baseline_start_date_ and `date` <= baseline_end_date_
    AND purchase_type = 'instore'
  GROUP BY product_id
)
, campaign_cost_tbl AS (
  -- guaranteed to have distinct product_id
  SELECT product_id
        , product_name
        , brand
        , category
        , IF(campaign_id IS NOT NULL, 1, 0) is_included_in_campaign
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
  , AVG(count_product) count_product # count of product included in this campaign as planned
        , SUM(cost_in_store) cost_in_store
        , SUM(cost_flyer) cost_flyer
        , SUM(cost_community) cost_community
        , SUM(cost_loyalty) cost_loyalty
        , SUM(cost_push) cost_push
        , SUM(cost_ad_total) cost_ad_total
  FROM (
    SELECT DISTINCT `date` 
        , product_id
        , product_name
        , brand
        , category
        , IF(campaign_id IS NOT NULL, 1, 0) is_included_in_campaign
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
        , count_product # count of product included in this campaign as planned
        , cost_in_store
        , cost_flyer
        , cost_community
        , cost_loyalty
        , cost_push
        , cost_ad_total
    FROM `positive-karma-457703-i3.retail_dashboard.dws_campaign_cost_sale_prd_d_inc` # 
    WHERE `date` >= post_campaign_start_date_
      AND `date` <= post_campaign_end_date_ 
      # AND campaign_id = campaign_id_ # it is already been sumed up in the last dws table for 7 days.
      # use dws_campaign_cost_sale_prd_d_inc will have Cartesian product between campaign event and sale type
      # e.g., campaign is conducted offline, however there is online sales 
      # so it is crucial to get distinct of all records first, remove the cartesian product.
      )
    GROUP BY product_id
          , product_name
          , brand
          , category
          , IF(campaign_id IS NOT NULL, 1, 0)
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
)
, halo_effect_tbl AS (
    SELECT product_id
        , IF(in_campaign_brand_tbl.brand IS NOT NULL, 1, 0) is_brand_halo_effect
        , IF(in_campaign_cat_tbl.category IS NOT NULL, 1, 0) is_category_halo_effect
    FROM `positive-karma-457703-i3.retail_dashboard.dim_product_full` AS prd_tbl
    LEFT JOIN (
      SELECT DISTINCT brand
      FROM campaign_cost_tbl
      WHERE campaign_id IS NOT NULL
    ) AS in_campaign_brand_tbl
  ON prd_tbl.brand = in_campaign_brand_tbl.brand
  LEFT JOIN (
    SELECT DISTINCT category
    FROM campaign_cost_tbl
    WHERE campaign_id IS NOT NULL
  ) AS in_campaign_cat_tbl
  ON prd_tbl.category = in_campaign_cat_tbl.category
)
, campaign_total_tbl AS (
   SELECT campaign_cost_tbl.product_id
        , campaign_cost_tbl.product_name
        , campaign_cost_tbl.brand
        , is_brand_halo_effect
        , campaign_cost_tbl.category
        , is_category_halo_effect
        , campaign_cost_tbl.is_included_in_campaign
        , campaign_cost_tbl.campaign_id
        , campaign_name
        , campaign_type
        , start_date
        , end_date
        , estimated_budget
        , approved_budget
        , real_spent_budget
        , discount
        , count_product
        , cost_in_store
        , cost_flyer
        , cost_community
        , cost_loyalty
        , cost_push
        , cost_ad_total
        , baseline_tbl.total_price_sum AS baseline_revenue
        , baseline_tbl.total_cost_sum AS baseline_cost
        , IF(campaign_id IS NOT NULL, post_campaign_tbl.total_discount_price_sum, post_campaign_tbl.total_price_sum) AS campaign_revenue # campaign use discounted sale price
        , post_campaign_tbl.total_cost_sum AS campaign_cost
        , estimated_sales_increase / 100 * baseline_tbl.total_price_sum AS estimated_incremental_revenue
        , IF(campaign_id IS NOT NULL, post_campaign_tbl.total_discount_price_sum, post_campaign_tbl.total_price_sum) - baseline_tbl.total_price_sum AS incremental_revenue
  FROM campaign_cost_tbl
  LEFT JOIN baseline_tbl
    ON campaign_cost_tbl.product_id = baseline_tbl.product_id
  LEFT JOIN post_campaign_tbl
    ON campaign_cost_tbl.product_id = post_campaign_tbl.product_id
  LEFT JOIN halo_effect_tbl
    ON campaign_cost_tbl.product_id = halo_effect_tbl.product_id
)

  SELECT '0' AS is_halo_effect
      , campaign_id
      , campaign_name
      , campaign_type
      , start_date
      , end_date
      , estimated_budget
      , approved_budget
      , real_spent_budget
      , count_product
      , AVG(discount) AS discount_avg
      , SUM(cost_in_store) AS cost_in_store
      , SUM(cost_flyer) AS cost_flyer
      , SUM(cost_community) AS cost_community
      , SUM(cost_loyalty) AS cost_loyalty
      , SUM(cost_push) AS cost_push
      , SUM(cost_ad_total) AS cost_ad_total
      , SUM(baseline_revenue) AS baseline_revenue
      , SUM(baseline_cost) AS baseline_cost
      , SUM(campaign_revenue) AS campaign_revenue
      , SUM(campaign_cost) AS campaign_product_cost
      , SUM(estimated_incremental_revenue) AS estimated_incremental_revenue 
      , SUM(incremental_revenue) AS incremental_revenue
  FROM campaign_total_tbl
  WHERE is_included_in_campaign = 1
    AND campaign_id = campaign_id_
  GROUP BY campaign_id
      , campaign_name
      , campaign_type
      , start_date
      , end_date
      , estimated_budget
      , approved_budget
      , real_spent_budget
      , count_product
UNION ALL
  SELECT is_halo_effect
        , campaign_info_tbl.campaign_id
        , campaign_name
        , campaign_type
        , start_date
        , end_date
        , estimated_budget
        , approved_budget
        , real_spent_budget
        , count_product
        , discount_avg
        , cost_in_store
        , cost_flyer
        , cost_community
        , cost_loyalty
        , cost_push
        , cost_ad_total
        , baseline_revenue
        , baseline_cost
        , campaign_revenue
        , campaign_product_cost
        , estimated_incremental_revenue
        , incremental_revenue
  FROM 
  (
    SELECT DISTINCT campaign_id
        , campaign_name
        , campaign_type
        , start_date
        , end_date
        , estimated_budget
        , approved_budget
        , real_spent_budget
        , count_product
    FROM campaign_total_tbl
    WHERE campaign_id = campaign_id_
  ) campaign_info_tbl
  LEFT JOIN 
  (
    SELECT 'all' as is_halo_effect # all sales and all cost for each campaign
        , campaign_id_ AS campaign_id
        , AVG(discount) AS discount_avg
        , SUM(cost_in_store) AS cost_in_store
        , SUM(cost_flyer) AS cost_flyer
        , SUM(cost_community) AS cost_community
        , SUM(cost_loyalty) AS cost_loyalty
        , SUM(cost_push) AS cost_push
        , SUM(cost_ad_total) AS cost_ad_total
        , SUM(baseline_revenue) AS baseline_revenue
        , SUM(baseline_cost) AS baseline_cost
        , SUM(campaign_revenue) AS campaign_revenue
        , SUM(campaign_cost) AS campaign_product_cost
        , SUM(estimated_incremental_revenue) AS estimated_incremental_revenue 
        , SUM(incremental_revenue) AS incremental_revenue
    FROM campaign_total_tbl
    UNION ALL
    SELECT '1' as is_halo_effect # only halo effect
        , campaign_id_ AS campaign_id
        , AVG(discount) AS discount_avg
        , SUM(cost_in_store) AS cost_in_store
        , SUM(cost_flyer) AS cost_flyer
        , SUM(cost_community) AS cost_community
        , SUM(cost_loyalty) AS cost_loyalty
        , SUM(cost_push) AS cost_push
        , SUM(cost_ad_total) AS cost_ad_total
        , SUM(baseline_revenue) AS baseline_revenue
        , SUM(baseline_cost) AS baseline_cost
        , SUM(campaign_revenue) AS campaign_revenue
        , SUM(campaign_cost) AS campaign_product_cost
        , SUM(estimated_incremental_revenue) AS estimated_incremental_revenue 
        , SUM(incremental_revenue) AS incremental_revenue
    FROM campaign_total_tbl
    WHERE is_brand_halo_effect = 1
      OR is_category_halo_effect = 1
  ) campaign_sum_tbl
  on campaign_info_tbl.campaign_id = campaign_sum_tbl.campaign_id