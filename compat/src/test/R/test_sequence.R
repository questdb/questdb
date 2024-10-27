library(RPostgres)
library(DBI)
library(testthat)

test_that("long_sequence(10) returns correct sequence", {
  con <- dbConnect(
    Postgres(),
    dbname = "qdb",
    host = "localhost",
    port = 8812,
    user = "admin",
    password = "quest"
  )

  # Run query
  result <- dbGetQuery(con, "SELECT * FROM long_sequence(10);")

  # Define expected result
  expected <- data.frame(x = 1:10)

  # Main test for exact match
  expect_equal(result, expected)

  # Additional assertions for specific properties
  expect_equal(nrow(result), 10)
  expect_true(all(diff(result$x) == 1))
  expect_equal(result$x[1], 1)
  expect_equal(result$x[10], 10)

  dbDisconnect(con)
})