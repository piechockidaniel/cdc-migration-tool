-- Aggregation transformation - Aggregate data before syncing
-- Useful for creating summary tables or data marts
-- Variables: {source_table}, {destination_table}, {group_by_columns}

-- Aggregate source data
CREATE OR REPLACE TEMP TABLE aggregated_data AS
SELECT
    -- Group by columns
    column1,
    column2,
    
    -- Aggregations
    COUNT(*) as record_count,
    SUM(amount) as total_amount,
    AVG(amount) as average_amount,
    MIN(date_column) as first_date,
    MAX(date_column) as last_date,
    
    -- Additional aggregations
    COUNT(DISTINCT customer_id) as unique_customers
    
FROM {source_table}
GROUP BY column1, column2
;

-- Output aggregated data
SELECT * FROM aggregated_data;

