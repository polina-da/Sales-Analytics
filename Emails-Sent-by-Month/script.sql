WITH dataset AS (

SELECT
DISTINCT es.id_account AS id_account,
DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH) AS sent_month,
MIN(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) OVER(PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS first_sent_date,
MAX(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) OVER(PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS last_sent_date,
COUNT(es.id_message) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS cnt_msg,
COUNT(es.id_message) OVER (PARTITION BY DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS msg_all
FROM `data-analytics-mate.DA.account_session` acs
JOIN `data-analytics-mate.DA.email_sent` es  
ON acs.account_id = es.id_account
JOIN `data-analytics-mate.DA.session` s  
ON acs.ga_session_id = s.ga_session_id
)

SELECT
sent_month,
id_account,
cnt_msg / msg_all * 100 AS sent_msg_percent_from_this_month,
first_sent_date,
last_sent_date
FROM dataset
