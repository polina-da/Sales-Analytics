WITH revenue_predict AS (
SELECT
s.date AS date,
SUM(p.price) AS revenue,
0 AS predict,
FROM `DA.session` s
JOIN `DA.order` o
ON s.ga_session_id = o.ga_session_id
JOIN `DA.product` p
ON o.item_id = p.item_id
GROUP BY date
UNION ALL
SELECT
rp.date AS date,
0 AS revenue,
SUM(rp.predict) AS predict,
FROM `DA.revenue_predict` rp
GROUP BY date
ORDER BY date
) ,


group_revenue_predict AS (
SELECT
date,
SUM(revenue) AS revenue,
SUM(predict) AS predict
FROM revenue_predict
GROUP BY date
) ,

mix_revenue_predict AS (
SELECT
date,
SUM(revenue) OVER(ORDER BY date) AS revenue,
SUM(predict) OVER(ORDER BY date) AS predict,
FROM group_revenue_predict
)

SELECT
date,
revenue,
predict,
revenue / predict * 100 AS percent
FROM mix_revenue_predict
