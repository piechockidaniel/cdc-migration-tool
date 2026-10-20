-- Pivot/Unpivot transformation - Reshape data structure
-- Transform between wide and narrow formats
-- Variables: {source_table}, {destination_table}

-- UNPIVOT Example: Convert columns to rows
CREATE OR REPLACE TEMP TABLE unpivoted_data AS
SELECT
    id_column,
    date_column,
    UNNEST(['metric1', 'metric2', 'metric3']) as metric_name,
    UNNEST([metric1_value, metric2_value, metric3_value]) as metric_value
FROM {source_table}
;

-- Or PIVOT Example: Convert rows to columns
-- CREATE OR REPLACE TEMP TABLE pivoted_data AS
-- SELECT
--     id_column,
--     MAX(CASE WHEN category = 'A' THEN value END) as category_a_value,
--     MAX(CASE WHEN category = 'B' THEN value END) as category_b_value,
--     MAX(CASE WHEN category = 'C' THEN value END) as category_c_value
-- FROM {source_table}
-- GROUP BY id_column
-- ;

-- Output transformed data
SELECT * FROM unpivoted_data;
-- SELECT * FROM pivoted_data;  -- For pivot version

