SELECT 
    c.city_name,
    d.year,
    d.month,
    AVG(f.cloud_cover) as avg_cloud_cover
FROM 
    fact_cloud_cover f
    JOIN dim_city c ON f.city_id = c.city_id
    JOIN dim_date d ON f.date_id = d.date_id
WHERE 
    d.year = 2020
GROUP BY 
    c.city_name, d.year, d.month
ORDER BY 
    c.city_name, d.month;