WITH accounts AS (
SELECT  
s.date AS date,
sp.country AS country,
a.send_interval AS send_interval,
a.is_verified AS is_verified,
a.is_unsubscribed AS is_unsubscribed,
COUNT(DISTINCT a.id) AS account_cnt
FROM `DA.session_params` sp
JOIN `DA.account_session` ac
ON sp.ga_session_id = ac.ga_session_id
JOIN `DA.account` a  
ON ac.account_id = a.id
JOIN `DA.session` s
ON ac.ga_session_id = s.ga_session_id
GROUP BY 1, 2, 3, 4, 5
) ,

account_metrics AS (
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
SUM(account_cnt) OVER(PARTITION BY country) AS total_country_account_cnt
FROM accounts
) ,

message_metrics AS (
SELECT
DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
sp.country AS country,
a.send_interval AS send_interval,
a.is_verified AS is_verified,
a.is_unsubscribed AS is_unsubscribed,
COUNT(DISTINCT es.id_message) AS sent_msg,
COUNT(DISTINCT eo.id_message) AS open_msg,
COUNT(DISTINCT ev.id_message) AS vist_msg
FROM `DA.email_sent` es
LEFT JOIN `DA.email_open` eo  
ON es.id_message = eo.id_message
LEFT JOIN `DA.email_visit` ev
ON es.id_message = ev.id_message
JOIN `DA.account_session` ac
ON es.id_account = ac.account_id
JOIN `DA.session_params` sp  
ON ac.ga_session_id = sp.ga_session_id
JOIN `DA.account` a
ON ac.account_id = a.id
JOIN `DA.session` s
ON sp.ga_session_id = s.ga_session_id
GROUP BY 1, 2, 3, 4, 5
) ,

message_metrics2 AS (
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
sent_msg,
open_msg,
vist_msg,
SUM(sent_msg) OVER(PARTITION BY country) AS total_country_sent_cnt  
FROM message_metrics
) ,

union_all AS (
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
0 AS sent_msg,
0 AS open_msg,
0 AS vist_msg,
total_country_account_cnt,
0 AS total_country_sent_cnt
FROM account_metrics
UNION ALL
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
0 AS account_cnt,
sent_msg,
open_msg,
vist_msg,
0 AS total_country_account_cnt,
total_country_sent_cnt
FROM message_metrics2
) ,

union_all2 AS (
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
sent_msg,
open_msg,
vist_msg,
total_country_account_cnt,
total_country_sent_cnt
FROM union_all
GROUP BY
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
sent_msg,
open_msg,
vist_msg,
total_country_account_cnt,
total_country_sent_cnt
) ,

ranking AS (
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
sent_msg,
open_msg,
vist_msg,
total_country_account_cnt,
total_country_sent_cnt,
DENSE_RANK() OVER(ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
DENSE_RANK() OVER(ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
FROM union_all2
)

SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
sent_msg,
open_msg,
vist_msg,
total_country_account_cnt,
total_country_sent_cnt,
rank_total_country_account_cnt,
rank_total_country_sent_cnt
FROM ranking
WHERE rank_total_country_account_cnt <=10 OR rank_total_country_sent_cnt <=10
