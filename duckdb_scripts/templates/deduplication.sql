-- Deduplication transformation - Remove duplicate records
-- Keeps the most recent record based on timestamp or sequence
-- Variables: {source_table}, {destination_table}, {unique_key}, {order_column}

-- Deduplicate source data (keep most recent)
CREATE OR REPLACE TEMP TABLE deduped_data AS
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY id_column  -- Change to your unique key column(s)
               ORDER BY timestamp_column DESC  -- Change to your ordering column
           ) as rn
    FROM {source_table}
) WHERE rn = 1
;

-- Remove the row_number column
ALTER TABLE deduped_data DROP COLUMN rn;

-- Output deduplicated data
SELECT * FROM deduped_data;

