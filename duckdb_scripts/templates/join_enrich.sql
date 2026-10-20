-- Join enrichment transformation - Join with reference data
-- Enrich source data by joining with lookup or reference tables
-- Variables: {source_table}, {destination_table}, {reference_table}

-- Note: Reference tables must be loaded into DuckDB first
-- You can load them from files or as temp tables

-- Join source with reference data
CREATE OR REPLACE TEMP TABLE enriched_data AS
SELECT
    s.*,
    -- Add columns from reference table
    r.category_name,
    r.category_description,
    r.region_name,
    r.is_active as category_active
FROM {source_table} s
LEFT JOIN reference_categories r 
    ON s.category_id = r.category_id
;

-- Multiple joins example:
-- CREATE OR REPLACE TEMP TABLE enriched_data AS
-- SELECT
--     s.*,
--     c.customer_name,
--     c.customer_tier,
--     p.product_name,
--     p.product_category
-- FROM {source_table} s
-- LEFT JOIN reference_customers c ON s.customer_id = c.customer_id
-- LEFT JOIN reference_products p ON s.product_id = p.product_id
-- ;

-- Output enriched data
SELECT * FROM enriched_data;

