-- keystone-input
-- The rows every rollup reads. `events` is the engine's view of silver events (DuckDB: Parquet
-- files; Flink: the Kafka topic). ADR-020: E1 files overlap E2 for 2015-01-01 00:00 to 08:00
-- UTC, so event-time windows drop E1 rows from that instant on and keep the E2 copy.
SELECT *
FROM events
WHERE NOT (era = 'E1' AND created_at >= TIMESTAMP '2015-01-01 00:00:00')
