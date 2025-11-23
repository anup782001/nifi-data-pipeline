-- Test 1: Verify record count
SELECT 
    'Record Count Test' as test_name,
    CASE 
        WHEN COUNT(*) = 5 THEN 'PASS'
        ELSE 'FAIL'
    END as result,
    COUNT(*) as actual_count,
    5 as expected_count
FROM customers;

-- Test 2: Verify data types and ranges
SELECT 
    'Data Type Validation' as test_name,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM customers
WHERE total_purchases < 0 
   OR LENGTH(email) > 255
   OR signup_date > CURRENT_DATE;

-- Test 3: Verify country values
SELECT 
    'Country Values Test' as test_name,
    CASE 
        WHEN COUNT(DISTINCT country) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as result,
    COUNT(DISTINCT country) as unique_countries
FROM customers;