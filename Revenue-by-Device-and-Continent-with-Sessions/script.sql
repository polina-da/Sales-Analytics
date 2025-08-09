WITH revenue AS (
SELECT
sp.continent,
SUM(p.price) AS revenue_all,
SUM(CASE WHEN sp.device = 'mobile' THEN p.price END) AS revenue_from_mobile,
SUM(CASE WHEN sp.device = 'desktop' THEN p.price END) AS revenue_from_desktop,
FROM `DA.order` o
JOIN `DA.product` p  
ON o.item_id = p.item_id
JOIN `DA.session_params` sp
ON o.ga_session_id = sp.ga_session_id
GROUP BY sp.continent
) ,

percent AS (
SELECT
continent,
revenue_all / SUM(revenue_all) OVER () * 100 AS percent_revenue_from_total
FROM revenue
) ,

acc_info AS (
SELECT
sp.continent,
COUNT(sp.ga_session_id) AS session_count,
COUNT(ac.account_id) AS account_count,
COUNT(CASE WHEN a.is_verified = 1 THEN ac.account_id END) AS verified_account
FROM `DA.session_params` sp
LEFT JOIN `DA.account_session` ac
ON sp.ga_session_id = ac.ga_session_id
LEFT JOIN `DA.account` a
ON ac.account_id = a.id
GROUP BY sp.continent
)

SELECT
revenue.continent,
revenue.revenue_all,
revenue.revenue_from_mobile,
revenue.revenue_from_desktop,
percent.percent_revenue_from_total,
acc_info.account_count,
acc_info.verified_account,
acc_info.session_count
FROM acc_info
LEFT JOIN revenue
ON acc_info.continent = revenue.continent
LEFT JOIN percent
ON acc_info.continent = percent.continent
