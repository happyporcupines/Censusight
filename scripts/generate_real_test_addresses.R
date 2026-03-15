#!/usr/bin/env Rscript

# -----------------------------------------------------------------------------
# File: scripts/generate_real_test_addresses.R
# Purpose: Generate realistic Wisconsin test CSV records from lookup data.
# Responsibilities:
# - Read sample addresses/GEOIDs from the local SQLite lookup table
# - Synthesize additional non-PII fields for testing workflows
# - Write a reproducible CSV fixture for manual/automated app testing
# -----------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
})

args <- commandArgs(trailingOnly = TRUE)

resolve_project_root <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)

  if (length(file_arg) >= 1) {
    script_path <- sub("^--file=", "", file_arg[[1]])
    return(normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = FALSE))
  }

  normalizePath(file.path(getwd(), ".."), winslash = "/", mustWork = FALSE)
}

project_root <- resolve_project_root()

get_arg <- function(flag, default = NULL) {
  idx <- which(args == flag)
  if (length(idx) == 0 || idx[1] == length(args)) return(default)
  args[idx[1] + 1]
}

n_rows <- as.integer(get_arg("--n", "200"))
out_path <- get_arg("--out", file.path("testing", "real_wi_test_addresses.csv"))
db_path <- get_arg("--db", file.path(project_root, "censu_app", "data", "address_geoid.sqlite"))

if (is.na(n_rows) || n_rows < 1) {
  stop("--n must be a positive integer")
}

if (!file.exists(db_path)) {
  stop(paste0("Lookup database not found: ", db_path))
}

con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

sample_sql <- paste0(
  "SELECT address, geoid ",
  "FROM address_geoid_lookup ",
  "WHERE geoid IS NOT NULL AND length(geoid) >= 11 ",
  "ORDER BY RANDOM() LIMIT ", n_rows
)

rows <- dbGetQuery(con, sample_sql)

if (!is.data.frame(rows) || nrow(rows) == 0) {
  stop("No valid rows found in address_geoid_lookup")
}

set.seed(20260301)
city_pool <- c("Milwaukee", "Madison", "Green Bay", "Kenosha", "Racine", "Appleton", "Eau Claire", "Janesville")

out_df <- data.frame(
  random.number = sample(1000:9999, nrow(rows), replace = TRUE),
  heart_rate = sample(55:110, nrow(rows), replace = TRUE),
  address = as.character(rows$address),
  city = sample(city_pool, nrow(rows), replace = TRUE),
  state = rep("WI", nrow(rows)),
  zip_code = sprintf("%05d", sample(53000:54999, nrow(rows), replace = TRUE)),
  stringsAsFactors = FALSE
)

write.csv(out_df, out_path, row.names = FALSE)

cat(sprintf("Created %d-row test CSV at %s\n", nrow(out_df), out_path))
