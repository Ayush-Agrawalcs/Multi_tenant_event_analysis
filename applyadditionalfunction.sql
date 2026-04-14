-- before indexing
EXPLAIN ANALYZE
SELECT *
FROM analytics.event
WHERE tenant_id = 't1';



CREATE INDEX idx_event_tenant
ON analytics.event (tenant_id);


-- after indexing
EXPLAIN ANALYZE
SELECT *
FROM analytics.event
WHERE tenant_id = 't1';


--before 
explain analyze
select * from analytics.event where properties->>'device'='mobile'

create index isx_event_device
on analytics.event((properties->'device'))

--after
explain analyze
select * from analytics.event where properties->>'device'='mobile'


EXPLAIN ANALYZE
SELECT *
FROM analytics.event;



EXPLAIN ANALYZE
SELECT *
FROM analytics.event
WHERE event_time >= '2026-04-01'
AND event_time < '2026-05-01';



EXPLAIN ANALYZE
SELECT tenant_id, COUNT(DISTINCT user_id)
FROM analytics.event
GROUP BY tenant_id;


EXPLAIN ANALYZE
SELECT *
FROM analytics.dau_mv;


ANALYZE analytics.event;