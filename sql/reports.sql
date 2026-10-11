-- 1. Top 10 product_id co luot view cao nhat trong ngay hien tai
SELECT dp.product_id, COUNT(*) AS views
FROM fact_page_view f
JOIN dim_product dp ON dp.product_key = f.product_key
JOIN dim_date dd ON dd.date_key = f.date_key
WHERE dd.full_date = CURRENT_DATE
GROUP BY dp.product_id
ORDER BY views DESC
LIMIT 10;

-- 2. Top 10 quoc gia co luot view cao nhat trong ngay hien tai
SELECT dl.country_name, COUNT(*) AS views
FROM fact_page_view f
JOIN dim_location dl ON dl.location_key = f.location_key
JOIN dim_date dd ON dd.date_key = f.date_key
WHERE dd.full_date = CURRENT_DATE
GROUP BY dl.country_name
ORDER BY views DESC
LIMIT 10;

-- 3. Top 5 referrer_url co luot view cao nhat trong ngay hien tai
SELECT f.referrer_url, COUNT(*) AS views
FROM fact_page_view f
JOIN dim_date dd ON dd.date_key = f.date_key
WHERE dd.full_date = CURRENT_DATE AND f.referrer_url IS NOT NULL AND f.referrer_url <> ''
GROUP BY f.referrer_url
ORDER BY views DESC
LIMIT 5;

-- 4. Voi 1 quoc gia bat ky, danh sach store_id va luot view, giam dan
--    (parameter: %(country)s = dl.country_name)
SELECT ds.store_id, COUNT(*) AS views
FROM fact_page_view f
JOIN dim_store ds ON ds.store_key = f.store_key
JOIN dim_location dl ON dl.location_key = f.location_key
WHERE dl.country_name = %(country)s
GROUP BY ds.store_id
ORDER BY views DESC;

-- 5. Du lieu view phan bo theo gio cua 1 product_id bat ky trong ngay
--    (parameter: %(product_id)s)
SELECT dd.hour, COUNT(*) AS views
FROM fact_page_view f
JOIN dim_product dp ON dp.product_key = f.product_key
JOIN dim_date dd ON dd.date_key = f.date_key
WHERE dp.product_id = %(product_id)s AND dd.full_date = CURRENT_DATE
GROUP BY dd.hour
ORDER BY dd.hour;

-- 6. Du lieu view theo gio cua tung browser, os
SELECT dd.hour, f.browser, f.os, COUNT(*) AS views
FROM fact_page_view f
JOIN dim_date dd ON dd.date_key = f.date_key
WHERE dd.full_date = CURRENT_DATE
GROUP BY dd.hour, f.browser, f.os
ORDER BY dd.hour, views DESC;
