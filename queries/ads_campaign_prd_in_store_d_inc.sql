/*
Name: ads_campaign_prd_in_store_full
Description: campaign summary based on product_id, purchase type only offline			
Data modeling reference: https://docs.google.com/spreadsheets/d/1FlbiZEBue2SCAUo1WOkdtX5WbEDyDrZ9B2uiZUSiRio/edit?gid=2043216767#gid=2043216767
Target Table: ads_campaign_prd_in_store_full
Source Table: dws_campaign_cost_sales_prd_d_inc
Created by: alvinxyzhang
Created Date: 2025-05-02
Version: v1.0
*/

-- SELECT * 
-- FROM `positive-karma-457703-i3.retail_dashboard.ads_campaign_prd_in_store_full`

-- SELECT coalesce(campaign_id, 19999), COUNT(1) row_cnt
-- FROM `positive-karma-457703-i3.retail_dashboard.ads_campaign_prd_in_store_full`
-- GROUP BY coalesce(campaign_id, 19999)

-- DECLARE date_ DATE DEFAULT DATE(2024, 1, 13); # any date of the campaign execution #  CURRENT_DATE()
DECLARE campaign_id_ DEFAULT 1;
DECLARE post_campaign_start_date_, post_campaign_end_date_, baseline_start_date_, baseline_end_date_, campaign_start_date_, campaign_end_date_ DATE;
DECLARE window_len_ DEFAULT 7; # window length = 1 week

SET (campaign_id_, post_campaign_start_date_, post_campaign_end_date_, baseline_start_date_, baseline_end_date_, campaign_start_date_, campaign_end_date_) = (
  SELECT AS STRUCT campaign_id # assupmtion: only one campaign happens during one period    
       , start_date AS post_campaign_start_date
       , DATE_ADD(end_date, INTERVAL window_len_ - 1 DAY) AS post_campaign_end_date # including today + the following 6 days = 1 week
      --  , date_ AS post_campaign_start_date
      --  , DATE_ADD(date_, INTERVAL window_len_ - 1 DAY) AS post_campaign_end_date # including today + the following 6 days = 1 week
       , DATE_SUB(start_date, INTERVAL campaign_length + window_len_ DAY) AS baseline_start_date
       , DATE_SUB(start_date, INTERVAL 1 DAY) AS baseline_end_date
       , start_date AS campaign_start_date
       , end_date AS campaign_end_date
  FROM (
    SELECT DISTINCT campaign_id, start_date, end_date, DATE_DIFF(end_date, start_date, DAY) + 1 campaign_length
    FROM `positive-karma-457703-i3.retail_dashboard.dwd_campaign_d_inc`
    WHERE campaign_id = campaign_id_
    -- WHERE start_date <= date_ 
      -- AND end_date >= date_
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
      SELECT DISTINCT 
            `date`
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
      FROM `positive-karma-457703-i3.retail_dashboard.dws_campaign_cost_sale_prd_d_inc`
      WHERE `date` >= campaign_start_date_ # it is already been sumed up in the last dws table for 7 days.
        AND `date` <= campaign_end_date_
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
, campaign_halo_cost_tbl AS (
    SELECT t1.brand
        , t1.category
        , halo_cost_in_store
        , halo_cost_flyer
        , halo_cost_community
        , halo_cost_loyalty
        , halo_cost_push
        , halo_cost_ad_total
    FROM 
    (
      SELECT DISTINCT brand, category
      FROM campaign_cost_tbl
    ) t1
    LEFT JOIN (
    SELECT brand, category
        , AVG(cost_in_store) halo_cost_in_store
        , AVG(cost_flyer) halo_cost_flyer
        , AVG(cost_community) halo_cost_community
        , AVG(cost_loyalty) halo_cost_loyalty
        , AVG(cost_push) halo_cost_push
        , AVG(cost_ad_total) halo_cost_ad_total
    FROM campaign_cost_tbl
    WHERE campaign_id IS NOT NULL
    GROUP BY brand, category
    ) t0
    ON t0.brand = t1.brand
    AND t0.category = t1.category
)

SELECT campaign_cost_tbl.product_id
      , campaign_cost_tbl.product_name
      , campaign_cost_tbl.brand
      , is_brand_halo_effect
      , campaign_cost_tbl.category
      , is_category_halo_effect
      , campaign_cost_tbl.is_included_in_campaign
      , coalesce(campaign_cost_tbl.campaign_id, campaign_id_) AS campaign_id
      , campaign_name
      , campaign_type
      , start_date
      , end_date
      , estimated_budget
      , approved_budget
      , real_spent_budget
      , discount
      , count_product
      , coalesce(cost_in_store, 0) AS cost_in_store
      , coalesce(cost_flyer, 0) AS cost_flyer
      , coalesce(cost_community, 0) AS cost_community
      , coalesce(cost_loyalty, 0) AS cost_loyalty
      , coalesce(cost_push, 0) AS cost_push
      , coalesce(cost_ad_total, 0) AS cost_ad_total
      , coalesce(baseline_tbl.total_price_sum, 0) AS baseline_revenue
      , coalesce(baseline_tbl.total_cost_sum, 0) AS baseline_cost
      , coalesce(IF(campaign_id IS NOT NULL, post_campaign_tbl.total_discount_price_sum, post_campaign_tbl.total_price_sum), 0) AS campaign_revenue # campaign use discounted sale price
      , coalesce(post_campaign_tbl.total_cost_sum, 0) AS campaign_cost
      , coalesce(estimated_sales_increase / 100 * baseline_tbl.total_price_sum, 0) AS estimated_revenue
      , coalesce(IF(campaign_id IS NOT NULL, post_campaign_tbl.total_discount_price_sum, post_campaign_tbl.total_price_sum) - baseline_tbl.total_price_sum, 0) AS incremental_revenue
      , coalesce(halo_cost_in_store, 0) AS halo_cost_in_store
      , coalesce(halo_cost_flyer, 0) AS halo_cost_flyer
      , coalesce(halo_cost_community, 0) AS halo_cost_community
      , coalesce(halo_cost_loyalty, 0) AS halo_cost_loyalty
      , coalesce(halo_cost_push, 0) AS halo_cost_push
      , coalesce(halo_cost_ad_total, 0) AS halo_cost_ad_total
FROM campaign_cost_tbl
LEFT JOIN baseline_tbl
  ON campaign_cost_tbl.product_id = baseline_tbl.product_id
LEFT JOIN post_campaign_tbl
  ON campaign_cost_tbl.product_id = post_campaign_tbl.product_id
LEFT JOIN halo_effect_tbl
  ON campaign_cost_tbl.product_id = halo_effect_tbl.product_id
LEFT JOIN campaign_halo_cost_tbl
  ON campaign_cost_tbl.brand = campaign_halo_cost_tbl.brand
  AND campaign_cost_tbl.category = campaign_halo_cost_tbl.category