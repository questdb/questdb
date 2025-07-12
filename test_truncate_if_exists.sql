-- Test script for TRUNCATE TABLE IF EXISTS functionality
-- This script demonstrates the new IF EXISTS clause for TRUNCATE TABLE

-- Create test tables
CREATE TABLE test_table AS (
    SELECT 
        timestamp_sequence(0, 1000000000) timestamp,
        x 
    FROM long_sequence(10)
) TIMESTAMP (timestamp);

-- Insert some data
INSERT INTO test_table SELECT 
    timestamp_sequence(1000000000, 1000000000) timestamp,
    x 
FROM long_sequence(5);

-- Show initial data
SELECT 'Initial data count:' as info, count() as count FROM test_table;

-- Test 1: Truncate existing table with IF EXISTS
TRUNCATE TABLE IF EXISTS test_table;
SELECT 'After truncate with IF EXISTS:' as info, count() as count FROM test_table;

-- Test 2: Try to truncate non-existing table with IF EXISTS (should not error)
TRUNCATE TABLE IF EXISTS non_existing_table;
SELECT 'Non-existing table truncate completed without error' as info;

-- Test 3: Try to truncate non-existing table without IF EXISTS (should error)
-- This will cause an error, so it's commented out
-- TRUNCATE TABLE non_existing_table;

-- Test 4: Truncate with KEEP SYMBOL MAPS and IF EXISTS
INSERT INTO test_table SELECT 
    timestamp_sequence(2000000000, 1000000000) timestamp,
    x 
FROM long_sequence(3);

SELECT 'Before truncate with KEEP SYMBOL MAPS:' as info, count() as count FROM test_table;

TRUNCATE TABLE IF EXISTS test_table KEEP SYMBOL MAPS;
SELECT 'After truncate with KEEP SYMBOL MAPS:' as info, count() as count FROM test_table;

-- Test 5: Multiple tables with IF EXISTS
CREATE TABLE test_table2 AS (
    SELECT 
        timestamp_sequence(0, 1000000000) timestamp,
        x 
    FROM long_sequence(5)
) TIMESTAMP (timestamp);

INSERT INTO test_table SELECT 
    timestamp_sequence(3000000000, 1000000000) timestamp,
    x 
FROM long_sequence(2);

SELECT 'Before multi-table truncate:' as info, 
       (SELECT count() FROM test_table) as table1_count,
       (SELECT count() FROM test_table2) as table2_count;

-- Truncate multiple tables, one exists, one doesn't
TRUNCATE TABLE IF EXISTS test_table, non_existing_table, test_table2;

SELECT 'After multi-table truncate:' as info, 
       (SELECT count() FROM test_table) as table1_count,
       (SELECT count() FROM test_table2) as table2_count;

-- Clean up
DROP TABLE test_table;
DROP TABLE test_table2; 