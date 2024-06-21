WITH customer_cte AS (
	SELECT DISTINCT
	    {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} as customer_id,
	    Country AS country
	FROM {{ ref('clean_retail') }}
	WHERE CustomerID IS NOT NULL
)
SELECT
    t.*,
	cm.iso
FROM customer_cte t
LEFT JOIN {{ ref('clean_country') }} cm ON t.country = cm.nicename

-- WITH customer_cte AS (
--     SELECT
--         {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} as customer_id,
--         Country AS country,
--         ROW_NUMBER() OVER (PARTITION BY {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} ORDER BY CustomerID) as row_num
--     FROM {{ source('retail', 'raw_online_retail_table') }}
--     WHERE CustomerID IS NOT NULL
-- )
-- SELECT
--     t.customer_id,
--     t.country,
--     cm.iso
-- FROM (
--     SELECT 
--         customer_id, 
--         country 
--     FROM customer_cte 
--     WHERE row_num = 1
-- ) t
-- LEFT JOIN {{ source('retail', 'country') }} cm ON t.country = cm.nicename
