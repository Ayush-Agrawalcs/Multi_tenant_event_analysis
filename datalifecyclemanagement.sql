DROP TABLE analytics.event_2026_01;


SELECT
    child.relname AS partition_name
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'event';

SELECT schemaname, tablename
FROM pg_tables
WHERE tablename = 'events_2026_04';

DROP TABLE public.events_2026_04;

-- Create archive table
CREATE TABLE analytics.event_archive AS
SELECT * FROM analytics.event_2026_01;