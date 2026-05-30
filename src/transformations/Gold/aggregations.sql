CREATE OR REFRESH MATERIALIZED VIEW gold_table
COMMENT "Gold layer aggregation of silver enriched table"
AS
SELECT id, name, category, count(*) as total_count 
FROM silver_enriched
GROUP BY id, name, category; 


