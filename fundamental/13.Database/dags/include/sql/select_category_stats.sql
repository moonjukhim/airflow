-- 카테고리별 상품 통계 (외부 SQL 파일 예제)
-- template_searchpath를 통해 SQLExecuteQueryOperator에서 참조됨

SELECT
    category,
    COUNT(*)              AS product_count,
    MIN(price)            AS min_price,
    MAX(price)            AS max_price,
    AVG(price)::INTEGER   AS avg_price,
    SUM(price)            AS total_price
FROM public.products
GROUP BY category
ORDER BY total_price DESC;
