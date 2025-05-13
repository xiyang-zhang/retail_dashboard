/*
k-means clustering using bigquery
*/


-- DECLARE date_ DATE DEFAULT DATE(2024, 1, 13); # any date of the campaign execution #  CURRENT_DATE()
DECLARE campaign_id_ DEFAULT 5;
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

CREATE MODEL
  `positive-karma-457703-i3.retail_dashboard.kmeans_customer`
OPTIONS
  ( MODEL_TYPE='KMEANS',
    NUM_CLUSTERS=5,
    KMEANS_INIT_METHOD='RANDOM')  
AS WITH daily_campaign_cost_tbl AS (
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
  FROM daily_campaign_cost_tbl
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
, post_campaign_tbl AS (
  SELECT sale_tbl.product_id
        , CASE
            WHEN age <= 19 THEN '<19'
            WHEN age >= 20 AND age <= 29 THEN '20 - 29'
            WHEN age >= 30 AND age <= 39 THEN '30 - 39'
            WHEN age >= 40 AND age <= 59 THEN '40 - 59'
            WHEN age >= 60 THEN '60+'
          END age_group
        , is_vip_membership
        , family_size
        , occupation
        , SUM(number_of_items) number_of_items_sum
        , AVG(retail_price) retail_price
        , AVG(retail_cost) retail_cost
        , SUM(total_price) total_price_sum
        , SUM(total_cost) total_cost_sum
        , AVG(retail_price * (1 - coalesce(discount, 0))) retail_discount_price
        , SUM(total_price * (1 - coalesce(discount, 0))) total_discount_price_sum
  FROM `positive-karma-457703-i3.retail_dashboard.dws_sale_customer_prd_d_inc` AS sale_tbl
  LEFT JOIN (
    SELECT `date`, product_id, discount
    FROM daily_campaign_cost_tbl
  ) discount_tbl
  ON sale_tbl.`date` = discount_tbl.`date`
  AND sale_tbl.product_id = discount_tbl.product_id
  WHERE sale_tbl.`date` >= post_campaign_start_date_ and sale_tbl.`date` <= post_campaign_end_date_
    AND purchase_type = 'instore'
  GROUP BY sale_tbl.product_id
        , CASE
            WHEN age <= 19 THEN '<19'
            WHEN age >= 20 AND age <= 29 THEN '20 - 29'
            WHEN age >= 30 AND age <= 39 THEN '30 - 39'
            WHEN age >= 40 AND age <= 59 THEN '40 - 59'
            WHEN age >= 60 THEN '60+'
          END
        , is_vip_membership
        , family_size
        , occupation
)
, baseline_tbl AS (
  SELECT sale_tbl.product_id
        , CASE
            WHEN age <= 19 THEN '<19'
            WHEN age >= 20 AND age <= 29 THEN '20 - 29'
            WHEN age >= 30 AND age <= 39 THEN '30 - 39'
            WHEN age >= 40 AND age <= 59 THEN '40 - 59'
            WHEN age >= 60 THEN '60+'
          END age_group
        , is_vip_membership
        , family_size
        , occupation
        , SUM(number_of_items) number_of_items_sum
        , AVG(retail_price) retail_price
        , AVG(retail_cost) retail_cost
        , SUM(total_price) total_price_sum
        , SUM(total_cost) total_cost_sum
        , AVG(retail_price * (1 - coalesce(discount, 0))) retail_discount_price
        , SUM(total_price * (1 - coalesce(discount, 0))) total_discount_price_sum
  FROM `positive-karma-457703-i3.retail_dashboard.dws_sale_customer_prd_d_inc` AS sale_tbl
  LEFT JOIN (
    SELECT `date`, product_id, discount
    FROM daily_campaign_cost_tbl
  ) discount_tbl
  ON sale_tbl.`date` = discount_tbl.`date`
  AND sale_tbl.product_id = discount_tbl.product_id
  WHERE sale_tbl.`date` >= baseline_start_date_ and sale_tbl.`date` <= baseline_end_date_
    AND purchase_type = 'instore'
  GROUP BY sale_tbl.product_id
        , CASE
            WHEN age <= 19 THEN '<19'
            WHEN age >= 20 AND age <= 29 THEN '20 - 29'
            WHEN age >= 30 AND age <= 39 THEN '30 - 39'
            WHEN age >= 40 AND age <= 59 THEN '40 - 59'
            WHEN age >= 60 THEN '60+'
          END
        , is_vip_membership
        , family_size
        , occupation
)
, user_campaign_tbl AS (
  SELECT age_group
      , is_vip_membership
      , family_size
      , occupation
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
      -- , SUM(estimated_incremental_revenue) AS estimated_incremental_revenue 
      , SUM(incremental_revenue) AS incremental_revenue
FROM (
  SELECT campaign_cost_tbl.product_id
      , baseline_tbl.age_group
      , baseline_tbl.is_vip_membership
      , baseline_tbl.family_size
      , baseline_tbl.occupation 
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
      -- , estimated_sales_increase / 100 * baseline_tbl.total_price_sum AS estimated_revenue
      , IF(campaign_id IS NOT NULL, post_campaign_tbl.total_discount_price_sum, post_campaign_tbl.total_price_sum) - baseline_tbl.total_price_sum AS incremental_revenue
FROM campaign_cost_tbl
LEFT JOIN baseline_tbl
  ON campaign_cost_tbl.product_id = baseline_tbl.product_id
LEFT JOIN post_campaign_tbl
  ON campaign_cost_tbl.product_id = post_campaign_tbl.product_id
  AND baseline_tbl.age_group = post_campaign_tbl.age_group
  AND baseline_tbl.is_vip_membership = post_campaign_tbl.is_vip_membership
  AND baseline_tbl.family_size = post_campaign_tbl.family_size
  AND baseline_tbl.occupation = post_campaign_tbl.occupation
)
GROUP BY age_group
      , is_vip_membership
      , family_size
      , occupation
)

SELECT age_group
  , is_vip_membership
  , family_size
  , occupation
  , coalesce(cost_in_store, 0) cost_in_store
  , coalesce(cost_flyer, 0) cost_flyer
  , coalesce(cost_community, 0) cost_community
  , coalesce(cost_loyalty, 0) cost_loyalty
  , coalesce(cost_push, 0) cost_push
  -- , coalesce(cost_ad_total, 0) cost_ad_total
  , coalesce(cost_in_store, 0) + coalesce(cost_flyer, 0) + coalesce(cost_community, 0) +  coalesce(cost_loyalty, 0) + coalesce(cost_push, 0)  total_campaign_cost
  , baseline_revenue
  , campaign_revenue
  , incremental_revenue
FROM user_campaign_tbl