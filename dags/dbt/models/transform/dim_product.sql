SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['StockCode', 'Description', 'UnitPrice']) }} as product_id,
		StockCode AS stock_code,
    Description AS description,
    UnitPrice AS price
FROM {{ ref('clean_retail') }}
WHERE StockCode IS NOT NULL
AND UnitPrice > 0