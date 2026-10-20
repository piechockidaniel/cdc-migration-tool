-- Basic filtering transformation - Filter rows based on conditions
-- This script filters the source data before syncing to destination
-- Variables: {source_table}, {destination_table}

-- Create filtered view of source data
CREATE OR REPLACE TEMP TABLE filtered_data AS
SELECT *
FROM {source_table}
WHERE 1=1
  -- Add your filter conditions here
  -- Example: AND status = 'ACTIVE'
  -- Example: AND created_date >= CURRENT_DATE - INTERVAL '30' DAY
;

-- Output filtered data for sync
SELECT * FROM filtered_data;

