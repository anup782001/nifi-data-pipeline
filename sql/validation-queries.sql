-- Data Quality Validation Queries

-- Check for duplicate emails
SELECT 
    'Duplicate Emails' as validation_check,
    COUNT(*) as issue_count
FROM (
    SELECT email, COUNT(*) as cnt
    FROM customers
    GROUP BY email
    HAVING COUNT(*) > 1
);

-- Check for NULL values in required fields
SELECT 
    'NULL Required Fields' as validation_check,
    COUNT(*) as issue_count
FROM customers
WHERE customer_id IS NULL 
   OR email IS NULL 
   OR signup_date IS NULL;

-- Check for invalid email formats
SELECT 
    'Invalid Email Format' as validation_check,
    COUNT(*) as issue_count
FROM customers
WHERE email NOT LIKE '%@%.%';

-- Check date ranges
SELECT 
    'Future Signup Dates' as validation_check,
    COUNT(*) as issue_count
FROM customers
WHERE signup_date > CURRENT_DATE;