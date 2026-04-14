
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

--Detect session gaps using LAG
SELECT
    user_id,
    event_time,
    LAG(event_time) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS prev_event_time,
    
    event_time - LAG(event_time) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS gap,
    
    CASE 
        WHEN event_time - LAG(event_time) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) > INTERVAL '30 minutes'
        THEN 'NEW SESSION'
        ELSE 'SAME SESSION'
    END AS session_flag

FROM analytics.event;

--Running total of events per user

SELECT
    user_id,
    event_time,
    COUNT(*) OVER (
        PARTITION BY user_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM analytics.event;


--Find users whose event count is greater than the average event count
SELECT user_id, COUNT(*) AS event_count
FROM analytics.event
GROUP BY user_id
HAVING COUNT(*) > (
    SELECT AVG(user_event_count)
    FROM (
        SELECT COUNT(*) AS user_event_count
        FROM analytics.event
        GROUP BY user_id
    ) sub
);


--Retrieve the latest event per user using a correlated subquery
SELECT *
FROM analytics.event e1
WHERE event_time = (
    SELECT MAX(e2.event_time)
    FROM analytics.event e2
    WHERE e2.user_id = e1.user_id
);



--Identify tenants whose total events exceed the overall average across tenants
SELECT tenant_id, COUNT(*) AS total_events
FROM analytics.event
GROUP BY tenant_id
HAVING COUNT(*) > (
    SELECT AVG(tenant_event_count)
    FROM (
        SELECT COUNT(*) AS tenant_event_count
        FROM analytics.event
        GROUP BY tenant_id
    ) sub
);

--Funnel analysis using multi-step CTEs

WITH signup AS (
    SELECT DISTINCT user_id
    FROM analytics.event
    WHERE event_name = 'signup'
),
cart AS (
    SELECT DISTINCT user_id
    FROM analytics.event
    WHERE event_name = 'add_to_cart'
),
purchase AS (
    SELECT DISTINCT user_id
    FROM analytics.event
    WHERE event_name = 'purchase'
)

SELECT
    (SELECT COUNT(*) FROM signup) AS signup_users,
    (SELECT COUNT(*) FROM cart) AS cart_users,
    (SELECT COUNT(*) FROM purchase) AS purchase_users;
	

--Retention calculation using CTEs
WITH first_day AS (
    SELECT user_id, MIN(DATE(event_time)) AS signup_day
    FROM analytics.event
    GROUP BY user_id
),
return_day AS (
    SELECT DISTINCT e.user_id
    FROM analytics.event e
    JOIN first_day f ON e.user_id = f.user_id
    WHERE DATE(e.event_time) = f.signup_day + 1
)

SELECT 
    COUNT(DISTINCT f.user_id) AS total_users,
    COUNT(DISTINCT r.user_id) AS retained_users,
    (COUNT(DISTINCT r.user_id) * 100.0 / COUNT(DISTINCT f.user_id)) AS retention_rate
FROM first_day f
LEFT JOIN return_day r ON f.user_id = r.user_id;



--Identify top-performing tenants over time using layered CTEs
WITH tenant_daily AS (
    SELECT 
        tenant_id,
        DATE(event_time) AS event_date,
        COUNT(*) AS daily_events
    FROM analytics.event
    GROUP BY tenant_id, DATE(event_time)
),
tenant_total AS (
    SELECT 
        tenant_id,
        SUM(daily_events) AS total_events
    FROM tenant_daily
    GROUP BY tenant_id
)

SELECT *
FROM tenant_total
ORDER BY total_events DESC
LIMIT 5;


--Combine CTE + window function to rank users within each tenant
WITH user_activity AS (
    SELECT 
        tenant_id,
        user_id,
        COUNT(*) AS total_events
    FROM analytics.event
    GROUP BY tenant_id, user_id
),
ranked_users AS (
    SELECT 
        tenant_id,
        user_id,
        total_events,
        RANK() OVER (
            PARTITION BY tenant_id
            ORDER BY total_events DESC
        ) AS rank
    FROM user_activity
)

SELECT *
FROM ranked_users
WHERE rank <= 5;


--Use subquery + JOIN to filter high-value users
SELECT u.user_id, u.total_spent
FROM (
    -- Step 1: Calculate total spending per user
    SELECT 
        user_id,
        SUM((properties->>'amount')::int) AS total_spent
    FROM analytics.event
    WHERE event_name = 'purchase'
    GROUP BY user_id
) u
JOIN (
    -- Step 2: Find average spending
    SELECT 
        AVG(total_spent) AS avg_spent
    FROM (
        SELECT 
            user_id,
            SUM((properties->>'amount')::int) AS total_spent
        FROM analytics.event
        WHERE event_name = 'purchase'
        GROUP BY user_id
    ) sub
) avg_table
ON u.total_spent > avg_table.avg_spent;

--Demonstrate partition pruning using EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT *
FROM analytics.event
WHERE tenant_id='1'
AND event_time BETWEEN '2026-04-01' AND '2026-04-30';





