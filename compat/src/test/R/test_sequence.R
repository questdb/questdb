install.packages(c("RPostgres", "DBI", "testthat"), repos = "https://cloud.r-project.org")

library(RPostgres)
library(DBI)
library(testthat)

# port from PGPORT environment variable or default
port <- Sys.getenv("PGPORT", 8812)

test_that("create - insert - select works", {
  con <- dbConnect(
    Postgres(),
    dbname = "qdb",
    host = "localhost",
    port = port,
    user = "admin",
    password = "quest"
  )

  dbExecute(con, "DROP TABLE IF EXISTS r_test;")
  dbExecute(con, "CREATE TABLE r_test(n int, ts timestamp) timestamp(ts) partition by hour;")
  dbExecute(con, "INSERT INTO r_test VALUES (1, '2024-10-28');")

  # Wait for WAL
  dbExecute(con, "select wait_wal_table('r_test');")

  result <- dbGetQuery(con, "SELECT * FROM r_test;")

  expected <- data.frame(n = 1, ts = as.POSIXct("2024-10-28", tz = "UTC"))

  expect_equal(result, expected)

  dbDisconnect(con)
})


test_that("2D arrays of doubles work correctly", {
  con <- dbConnect(
    Postgres(),
    dbname = "qdb",
    host = "localhost",
    port = port,
    user = "admin",
    password = "quest"
  )
  
  # Create a table with a 2D array column of doubles
  dbExecute(con, "DROP TABLE IF EXISTS array_test;")
  dbExecute(con, "CREATE TABLE array_test(id int, data double[][]);")
  
  # Create a 2D array of doubles to insert
  # This creates a 2x3 array: {{1.1, 2.2, 3.3}, {4.4, 5.5, 6.6}}
  dbExecute(con, "INSERT INTO array_test VALUES (1, ARRAY[[1.1, 2.2, 3.3], [4.4, 5.5, 6.6]]);")
  
  # Wait for WAL
  dbExecute(con, "SELECT wait_wal_table('array_test');")
  
  # Query and check the result
  result <- dbGetQuery(con, "SELECT * FROM array_test;")
  
  # Expected result - the array should be returned as a string in PostgreSQL format
  # which will be "{{{1.1,2.2,3.3},{4.4,5.5,6.6}}}" or similar
  expect_equal(result$id, 1)
  
  # Parse the array string into a proper R array for verification
  # The format may vary slightly depending on how R/PostgreSQL represents the array
  array_string <- result$data[1]
  
  # We need to check the array was stored and retrieved correctly
  # One approach is to query individual elements to verify they match what we inserted
  element_check <- dbGetQuery(con, "SELECT data[1][1], data[1][3], data[2][2] FROM array_test WHERE id = 1;")
  expect_equal(element_check[[1]], 1.1)
  expect_equal(element_check[[2]], 3.3)
  expect_equal(element_check[[3]], 5.5)
  
  # Test with a more complex 2D array with varying precision
  dbExecute(con, "INSERT INTO array_test VALUES (2, ARRAY[[1.123456789, 2.987654321], [3.14159265359, 4.0000000001]]);")
  dbExecute(con, "SELECT wait_wal_table('array_test');")
  
  # Check precision is maintained
  precision_check <- dbGetQuery(con, "SELECT data[1][1], data[1][2], data[2][1], data[2][2] FROM array_test WHERE id = 2;")
  expect_equal(precision_check[[1]], 1.123456789)
  expect_equal(precision_check[[2]], 2.987654321)
  expect_equal(precision_check[[3]], 3.14159265359)
  expect_equal(precision_check[[4]], 4.0000000001)
  
  # Clean up
  dbExecute(con, "DROP TABLE IF EXISTS array_test;")
  dbDisconnect(con)
})