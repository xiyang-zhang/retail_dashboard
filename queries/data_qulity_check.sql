SELECT *
FROM 
(SELECT `date`, sum(coalesce(total_discount_price, 0)) discount_revenue, sum(total_price) revenue
FROM `positive-karma-457703-i3.retail_dashboard.dws_campaign_cost_sale_prd_d_inc`
WHERE `date` >= DATE(2025,4,1)
  AND `date` <= DATE(2025,4,25)
GROUP BY `date`) AS tbl1
LEFT JOIN 
(SELECT `date`, sum(total_price) revenue_sale
FROM `positive-karma-457703-i3.retail_dashboard.dws_sale_customer_prd_d_inc`
WHERE `date` >= DATE(2025,4,1)
  AND `date` <= DATE(2025,4,25)
GROUP BY `date`) AS tbl2
ON tbl1.`date` = tbl2.`date`
ORDER BY tbl1.`date` ASC