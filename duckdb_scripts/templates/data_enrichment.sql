-- Data enrichment transformation - Add calculated columns and derived data
-- Enrich source data with additional computed fields
-- Variables: {source_table}, {destination_table}

-- Enrich data with calculations
CREATE OR REPLACE TEMP TABLE enriched_data AS
SELECT
    *,
    
    -- Add calculated columns
    CASE 
        WHEN amount > 1000 THEN 'HIGH'
        WHEN amount > 500 THEN 'MEDIUM'
        ELSE 'LOW'
    END as amount_category,
    
    -- Date/time calculations
    EXTRACT(YEAR FROM date_column) as year,
    EXTRACT(MONTH FROM date_column) as month,
    EXTRACT(QUARTER FROM date_column) as quarter,
    DATE_TRUNC('month', date_column) as month_start,
    
    -- String manipulations
    UPPER(name_column) as name_upper,
    LENGTH(description) as description_length,
    
    -- Conditional logic
    CASE 
        WHEN status = 'COMPLETED' AND verified = true THEN 'VERIFIED'
        WHEN status = 'COMPLETED' THEN 'UNVERIFIED'
        ELSE status
    END as final_status,
    
    -- Current timestamp
    CURRENT_TIMESTAMP as enriched_at
    
FROM {source_table}
;

-- Output enriched data
SELECT * FROM enriched_data;

