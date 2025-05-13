/*
Name: insert_dws_campaign_cost_prd_d_inc
Description: insert data to dws_campaign_cost_prd_d_inc. Sum up cost of each product during each campaign, including total budget and it remaining budget for the previous 7 days. 
Data modeling reference: https://docs.google.com/spreadsheets/d/1FlbiZEBue2SCAUo1WOkdtX5WbEDyDrZ9B2uiZUSiRio/edit?gid=801113373#gid=801113373
Target Table: dws_campaign_cost_prd_d_inc
Source Table: dwd_ads_d_inc, dwd_budget_d_in, dwd_campaign_d_inc
Created by: alvinxyzhang
Created Date: 2025-04-28
Version: v1.0
*/

DECLARE start_date_ DATE DEFAULT DATE_SUB(DATE(2024, 2, 21), INTERVAL 7 DAY); # CURRENT_DATE()
DECLARE end_date_ DATE DEFAULT DATE(2024, 2, 21); # any date of the campaign execution #  CURRENT_DATE()
-- DECLARE campaign_id_ INT64 DEFAULT 1; # campaign_id
-- SET campaign_id_=3;

WITH campaign_tbl AS (
  -- campaign summary table
  SELECT campaign_id
      , campaign_name
      , campaign_type
      , start_date
      , end_date
      , estimated_budget
      , approved_budget
      , real_spent_budget
      , product_id
      , discount
      , estimated_sales_increase
      , COUNT(DISTINCT product_id) OVER (PARTITION BY campaign_id) count_product
  FROM `positive-karma-457703-i3.retail_dashboard.dwd_campaign_d_inc`
  WHERE (start_date <= start_date_ and end_date >= start_date_)
    OR (start_date <= end_date_ and end_date >= end_date_)
  --  AND campaign_id = campaign_id_
)
, ad_tbl AS (
  -- advertise to sum the cost over the last 7 days, linear attribution to each product. 
  SELECT campaign_id 
      , ad_group
      , product_id
      , agency_or_specialist
      , SUM(cost) AS cost_ad_total
      , SUM(IF(ads_campaign_type = "online-search", cost, 0))  AS cost_ad_search
      , SUM(IF(ads_campaign_type = "online-displays", cost, 0))  AS cost_ad_display
      , SUM(IF(ads_campaign_type = "online-event", cost, 0))  AS cost_ad_event
      , SUM(IF(ads_campaign_type = "retargeting-ads", cost, 0))  AS cost_ad_retargeting
      , SUM(IF(ads_campaign_type = "email-ads", cost, 0))  AS cost_ad_email
      , SUM(impressions) AS impressions_ad_total
      , SUM(IF(ads_campaign_type = "online-search", impressions, 0))  AS impressions_ad_search
      , SUM(IF(ads_campaign_type = "online-displays", impressions, 0))  AS impressions_ad_display
      , SUM(IF(ads_campaign_type = "online-event", impressions, 0))  AS impressions_ad_event
      , SUM(IF(ads_campaign_type = "retargeting-ads", impressions, 0))  AS impressions_ad_retargeting
      , SUM(IF(ads_campaign_type = "email-ads", impressions, 0))  AS impressions_ad_email
      , SUM(clicks) AS clicks_ad_total
      , SUM(IF(ads_campaign_type = "online-search", clicks, 0))  AS clicks_ad_search
      , SUM(IF(ads_campaign_type = "online-displays", clicks, 0))  AS clicks_ad_display
      , SUM(IF(ads_campaign_type = "online-event", clicks, 0))  AS clicks_ad_event
      , SUM(IF(ads_campaign_type = "retargeting-ads", clicks, 0))  AS clicks_ad_retargeting
      , SUM(IF(ads_campaign_type = "email-ads", clicks, 0))  AS clicks_ad_email
      , SUM(conversions) AS conversions_ad_total
      , SUM(IF(ads_campaign_type = "online-search", conversions, 0))  AS conversions_ad_search
      , SUM(IF(ads_campaign_type = "online-displays", conversions, 0))  AS conversions_ad_display
      , SUM(IF(ads_campaign_type = "online-event", conversions, 0))  AS conversions_ad_event
      , SUM(IF(ads_campaign_type = "retargeting-ads", conversions, 0))  AS conversions_ad_retargeting
      , SUM(IF(ads_campaign_type = "email-ads", conversions, 0))  AS conversions_ad_email
  FROM `positive-karma-457703-i3.retail_dashboard.dwd_ads_d_inc`
  WHERE `date` >= start_date_
    AND `date` <= end_date_
  GROUP BY campaign_id
        , ad_group
        , product_id
        , agency_or_specialist
)
, budget_tbl AS (
  -- budget summation of the past one week including today
  SELECT campaign_id
      , agency_or_specialist
      , product_id
      , SUM(cost_in_store) AS cost_in_store
      , SUM(cost_flyer) AS cost_flyer
      , SUM(cost_community) AS cost_community
      , SUM(cost_loyalty) AS cost_loyalty
      , SUM(cost_push) AS cost_push
  FROM (
    SELECT `date`
        , campaign_id
        , agency_or_specialist
        , product_id
        , SUM(IF(activity = "In-store Setup", spent, 0)) / AVG(product_list_len) AS cost_in_store
        , SUM(IF(activity = "Flyer Launch", spent, 0)) / AVG(product_list_len) AS cost_flyer
        , SUM(IF(activity = "Local Community Events", spent, 0)) / AVG(product_list_len) AS cost_community
        , SUM(IF(activity = "Loyalty Program", spent, 0)) / AVG(product_list_len) AS cost_loyalty
        , SUM(IF(activity = "Digital Push", spent, 0)) / AVG(product_list_len) AS cost_push
    FROM `positive-karma-457703-i3.retail_dashboard.dwd_budget_d_inc`
    WHERE `date` >= start_date_
      AND `date` <= end_date_
    GROUP BY `date`
          , campaign_id
          , agency_or_specialist  
          , product_id
  ) as t0
  GROUP BY campaign_id
        , agency_or_specialist
        , product_id
)

SELECT end_date as `date`
    , campaign_tbl.campaign_id
    , campaign_tbl.product_id
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
    -- budget_tbl
    , budget_tbl.agency_or_specialist
    , cost_in_store
    , cost_flyer
    , cost_community
    , cost_loyalty
    , cost_push
    -- ads_tbl
    , ad_group
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
FROM campaign_tbl
LEFT JOIN budget_tbl 
-- including all planned product_id
  ON campaign_tbl.campaign_id = budget_tbl.campaign_id
  AND campaign_tbl.product_id = budget_tbl.product_id
LEFT JOIN ad_tbl
  ON campaign_tbl.campaign_id = ad_tbl.campaign_id
  AND campaign_tbl.product_id = ad_tbl.product_id
  AND ad_tbl.campaign_id = budget_tbl.campaign_id
  AND ad_tbl.product_id = budget_tbl.product_id
  AND ad_tbl.agency_or_specialist = budget_tbl.agency_or_specialist
-- WHERE agency_or_specialist IS NOT NULL