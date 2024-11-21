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