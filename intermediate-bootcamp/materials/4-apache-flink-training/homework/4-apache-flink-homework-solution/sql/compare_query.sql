-- Q1: What is the average number of web events of a session from a user on Tech Creator?
-- This computes the overall average session size (in events) 
-- across all sessions that happened on any Tech Creator host.
SELECT 
    AVG(num_hits) AS avg_events_per_session
FROM processed_sessions_aggregated
WHERE host LIKE '%techcreator%';


-- Q2: Compare results between different hosts
-- This groups sessions by host, then calculates the average session size per host.
-- It specifically compares the three mentioned hosts and orders them by engagement.
SELECT 
    host,
    AVG(num_hits) AS avg_events_per_session
FROM processed_sessions_aggregated
WHERE host IN (
    'zachwilson.techcreator.io',
    'zachwilson.tech',
    'lulu.techcreator.io'
)
GROUP BY host
ORDER BY avg_events_per_session DESC;


-- Q3: Average number of web events per session per user on Tech Creator
-- This adds "ip" (the user identifier) to the analysis. 
-- It computes the average number of events per session for each individual user (IP),
-- restricted to Tech Creator hosts.
SELECT 
    ip,
    AVG(num_hits) AS avg_events_per_session
FROM processed_sessions_aggregated
WHERE host LIKE '%techcreator%'
GROUP BY ip;
