# DuckDB Transformation Scripts Repository

This directory contains DuckDB SQL scripts for in-memory data transformations during the synchronization process.

## Directory Structure

```
duckdb_scripts/
├── templates/          # Pre-built template scripts
│   ├── basic_filter.sql
│   ├── aggregation.sql
│   ├── deduplication.sql
│   ├── data_enrichment.sql
│   ├── pivot_unpivot.sql
│   └── join_enrich.sql
├── custom/            # User-created custom scripts
└── scripts_index.json # Script metadata index
```

## Template Scripts

### 1. `basic_filter.sql`
**Purpose:** Filter rows based on conditions before syncing

**Use Cases:**
- Exclude inactive records
- Filter by date ranges
- Apply business rules

**Example:**
```sql
SELECT * FROM source_data
WHERE status = 'ACTIVE'
  AND created_date >= CURRENT_DATE - INTERVAL '30' DAY
```

### 2. `aggregation.sql`
**Purpose:** Aggregate data for summary tables

**Use Cases:**
- Create data marts
- Daily/monthly summaries
- Rollup calculations

**Example:**
```sql
SELECT 
    customer_id,
    DATE_TRUNC('day', order_date) as day,
    COUNT(*) as order_count,
    SUM(total_amount) as total_sales
FROM source_data
GROUP BY customer_id, DATE_TRUNC('day', order_date)
```

### 3. `deduplication.sql`
**Purpose:** Remove duplicate records

**Use Cases:**
- Keep most recent record
- Remove exact duplicates
- Consolidate data

**Example:**
```sql
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) as rn
    FROM source_data
) WHERE rn = 1
```

### 4. `data_enrichment.sql`
**Purpose:** Add calculated columns and derived fields

**Use Cases:**
- Categorization
- Date/time extraction
- String manipulation
- Conditional logic

**Example:**
```sql
SELECT 
    *,
    CASE WHEN amount > 1000 THEN 'HIGH' ELSE 'LOW' END as tier,
    EXTRACT(YEAR FROM date_col) as year,
    LENGTH(description) as desc_length
FROM source_data
```

### 5. `pivot_unpivot.sql`
**Purpose:** Reshape data structure

**Use Cases:**
- Convert wide to narrow format
- Convert narrow to wide format
- Normalize/denormalize data

**Example (Unpivot):**
```sql
SELECT 
    id,
    UNNEST(['Q1', 'Q2', 'Q3', 'Q4']) as quarter,
    UNNEST([q1_sales, q2_sales, q3_sales, q4_sales]) as sales
FROM source_data
```

### 6. `join_enrich.sql`
**Purpose:** Join with reference/lookup data

**Use Cases:**
- Add category names
- Enrich with master data
- Resolve foreign keys

**Example:**
```sql
SELECT 
    s.*,
    c.customer_name,
    p.product_name
FROM source_data s
LEFT JOIN customers c ON s.customer_id = c.id
LEFT JOIN products p ON s.product_id = p.id
```

## Creating Custom Scripts

### Script Format

```sql
-- Description of what this script does (first line)
-- This line becomes the script description in the UI

-- Your transformation logic here
CREATE OR REPLACE TEMP TABLE transformed_data AS
SELECT 
    -- Your columns and transformations
FROM {source_table}
WHERE -- Your conditions
;

-- MUST end with SELECT statement to output data
SELECT * FROM transformed_data;
```

### Variables Available

Scripts can use these placeholder variables (substituted before execution):
- `{source_table}` - the DuckDB staging table holding the current batch (CDC batch or snapshot chunk)
- `{destination_table}` - name of the destination table
- `{mapping_id}` - ID of the current mapping

Execution contract (2.0): statements are executed in order; the **last SELECT** defines the rows
written to the destination. Output columns are renamed through the mapping's column mappings,
so scripts may keep source column names. Scripts run for INSERT/UPDATE batches and snapshot
chunks; DELETEs are routed by key columns without the script.

### Best Practices

1. **Always use TEMP tables** for intermediate results
2. **End with SELECT** to output final data
3. **Add description** in first comment line
4. **Handle NULLs** explicitly
5. **Test with sample data** before production use
6. **Keep scripts focused** - one transformation per script
7. **Comment complex logic** for maintainability

### Example Custom Script

File: `custom/customer_enrichment.sql`

```sql
-- Enrich customer data with tier and lifetime value calculations

-- Calculate customer lifetime value
CREATE OR REPLACE TEMP TABLE customer_metrics AS
SELECT 
    customer_id,
    COUNT(*) as total_orders,
    SUM(order_amount) as lifetime_value,
    AVG(order_amount) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM {source_table}
GROUP BY customer_id
;

-- Assign tiers
CREATE OR REPLACE TEMP TABLE customer_enriched AS
SELECT 
    *,
    CASE 
        WHEN lifetime_value > 10000 THEN 'PLATINUM'
        WHEN lifetime_value > 5000 THEN 'GOLD'
        WHEN lifetime_value > 1000 THEN 'SILVER'
        ELSE 'BRONZE'
    END as customer_tier,
    DATEDIFF('day', first_order_date, last_order_date) as customer_age_days
FROM customer_metrics
;

-- Output enriched data
SELECT * FROM customer_enriched;
```

## Using Scripts in Mappings

### Via the mapping

Set `use_duckdb_transformation = true` and either `duckdb_script_name` (file in `custom/` or
`templates/`, managed through `/api/admin/duckdb/scripts/…`) or `duckdb_script_content`
(inline). See the [mappings guide](../docs/user-guide/04-mappings.md).

### Via API

```python
mapping = {
    "id": "customers_with_transformation",
    "source_schema": "dbo",
    "source_table": "Customers",
    "destination_schema": "dw",
    "destination_table": "DimCustomers",
    "use_duckdb_transformation": True,
    "duckdb_script_name": "customer_enrichment",  # From custom/
    # OR provide inline script:
    # "duckdb_script_content": "SELECT * FROM {source_table} WHERE active = true",
    "column_mappings": [
        # ... your column mappings
    ]
}
```

## DuckDB Functions & Features

DuckDB provides rich SQL functionality:

### String Functions
```sql
UPPER(col), LOWER(col), LENGTH(col)
CONCAT(col1, ' ', col2)
SUBSTRING(col, 1, 10)
REGEXP_MATCHES(col, 'pattern')
```

### Date/Time Functions
```sql
CURRENT_DATE, CURRENT_TIMESTAMP
DATE_TRUNC('month', date_col)
EXTRACT(YEAR FROM date_col)
DATEDIFF('day', date1, date2)
date_col + INTERVAL '30' DAY
```

### Aggregations
```sql
COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)
COUNT(DISTINCT col)
STRING_AGG(col, ',')
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)
```

### Window Functions
```sql
ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2)
RANK() OVER (ORDER BY col)
LAG(col) OVER (ORDER BY date_col)
LEAD(col) OVER (ORDER BY date_col)
```

### JSON Functions
```sql
JSON_EXTRACT(json_col, '$.field')
JSON_ARRAY(col1, col2, col3)
TO_JSON(struct_col)
```

### Array Functions
```sql
UNNEST(array_col)
ARRAY_AGG(col)
LIST_VALUE(1, 2, 3)
```

## Performance Tips

1. **Filter early** - Apply WHERE clauses as soon as possible
2. **Use indexes** - DuckDB auto-indexes, but be mindful of data volume
3. **Limit intermediate results** - Use LIMIT for testing
4. **Avoid SELECT *** - Specify needed columns
5. **Monitor memory** - DuckDB is in-memory, watch for large datasets
6. **Test with samples** - Use `LIMIT 1000` during development

## Testing Scripts

### Test in DuckDB CLI

```bash
duckdb
```

```sql
-- Load test data
CREATE TABLE test_source AS 
SELECT * FROM read_csv_auto('test_data.csv');

-- Run your script
-- ... your transformation SQL ...

-- Check results
SELECT COUNT(*) FROM result_table;
SELECT * FROM result_table LIMIT 10;
```

### Test via Python

```python
import duckdb

# Connect
conn = duckdb.connect(':memory:')

# Load test data
conn.execute("CREATE TABLE source_data AS SELECT * FROM read_csv('test.csv')")

# Run script
with open('duckdb_scripts/custom/my_script.sql') as f:
    script = f.read()
    result = conn.execute(script).fetchdf()

print(result.head())
```

## Troubleshooting

### Common Issues

**Issue:** Script fails with "table not found"
**Solution:** Ensure you're using `{source_table}` placeholder correctly

**Issue:** Memory error with large datasets
**Solution:** Add filters, use sampling, or process in batches

**Issue:** Syntax error
**Solution:** Test script in DuckDB CLI first

**Issue:** Wrong column names
**Solution:** Check source table schema, column names are case-sensitive

## Examples Repository

See `examples/duckdb_transformation_example.py` for complete working examples of:
- Loading scripts
- Creating custom scripts
- Using transformations in mappings
- Testing transformations

## Support

For DuckDB SQL reference: https://duckdb.org/docs/

For issues or questions about transformation scripts, check the logs or create an issue in the repository.

