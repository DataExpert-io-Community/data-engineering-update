SELECT * FROM processed_events;
SELECT * FROM processed_events_aggregated;
SELECT distinct(host) FROM processed_events_aggregated_source;


CREATE TABLE processed_events_aggregated(
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
)

CREATE TABLE processed_events_aggregated_source(
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
)