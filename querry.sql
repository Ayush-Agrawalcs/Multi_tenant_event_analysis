
--Unique users per day per tenant
SELECT tenant_id, DATE(event_time), count (DISTINCT user_id)
FROM analytics.event
GROUP BY tenant_id, DATE(event_time);

 
--Funnel Analysis
select
count(Distinct case when event_name='signup' then user_id End)as signup,
  COUNT(DISTINCT CASE WHEN event_name='add_to_cart' THEN user_id END) AS cart,
    COUNT(DISTINCT CASE WHEN event_name='purchase' THEN user_id END) AS purchase,
	 COUNT(DISTINCT CASE WHEN event_name='view' THEN user_id END) AS vie
FROM analytics.event;


--retention
WITH first_day AS (
    SELECT user_id, MIN(DATE(event_time)) AS signup_day
    FROM analytics.event
    GROUP BY user_id
),
return_day AS (
    SELECT e.user_id
    FROM analytics.event e 
    JOIN first_day f ON e.user_id = f.user_id
    WHERE DATE(e.event_time) = f.signup_day + 1
)
SELECT COUNT(*) FROM return_day;

-- Advance querry

--Top 5 most active users per tenant
WITH user_counts AS (
    SELECT 
        tenant_id,
        user_id,
        COUNT(*) AS event_count,
        Row_Number() OVER (
            PARTITION BY tenant_id 
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM analytics.event
    GROUP BY tenant_id, user_id
)

SELECT 
    tenant_id,
    user_id,
    event_count
FROM user_counts
WHERE rn <= 5;

--Event distribution per tenant by event type
WITH name_counts AS (
    SELECT 
        tenant_id,
    count(Distinct case when event_name='signup' then user_id End)as signup,
	COUNT(DISTINCT CASE WHEN event_name='add_to_cart' THEN user_id END) AS cart,
    COUNT(DISTINCT CASE WHEN event_name='purchase' THEN user_id END) AS purchase,
	 COUNT(DISTINCT CASE WHEN event_name='view' THEN user_id END) AS vie
    FROM analytics.event
    GROUP BY tenant_id
)
SELECT *
FROM name_counts
order by tenant_id asc


--Total revenue per tenant (from JSONB field)
with total_revenue as(
	select tenant_id,sum((properties->>'amount')::int)as s
	from analytics.event
	WHERE event_name = 'purchase'
	group by tenant_id
)
select *
from total_revenue


--Users with no activity (LEFT JOIN + NULL filtering)
select u.user_id,u.tenant_id
from analytics.user u
left join analytics.event e
on u.user_id=e.user_id
and u.tenant_id=e.tenant_id
where e.user_id is Null


--Identify users associated with multiple tenants
SELECT user_id, COUNT(DISTINCT tenant_id) AS ci
FROM analytics.event
GROUP BY user_id
HAVING COUNT(DISTINCT tenant_id) > 1;


--First event per user using ROW_NUMBER
SELECT *
FROM (
    SELECT 
        user_id,
        tenant_id,
        event_name,
        event_time,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY event_time ASC
        ) AS rn
    FROM analytics.event
) t
WHERE rn = 1;










