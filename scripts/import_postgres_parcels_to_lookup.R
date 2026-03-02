# -----------------------------------------------------------------------------
# File: scripts/import_postgres_parcels_to_lookup.R
# Purpose: Import parcel address/GEOID records from Postgres via RPostgres.
# Responsibilities:
# - Connect to Postgres and discover source columns
# - Stream source records in chunks for memory-safe processing
# - Normalize addresses/GEOIDs and upsert into SQLite lookup cache
# - Persist import metadata for auditing and troubleshooting
# - helps bring in the data for build_address_geoid_sqlite.R from a postgres database instead of a csv
# -----------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(dplyr)
})

if (!requireNamespace("RPostgres", quietly = TRUE)) {
  stop("Package 'RPostgres' is required. Install with install.packages('RPostgres').")
}

normalize_address <- function(x) {
  x <- tolower(trimws(as.character(x)))
  x <- gsub("[[:punct:]]", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

normalize_geoid <- function(x) {
  x <- gsub("[^0-9]", "", as.character(x))
  ifelse(nchar(x) >= 11, x, NA_character_)
}

pick_first_existing <- function(candidates, values) {
  match_idx <- match(tolower(candidates), tolower(values), nomatch = 0)
  hit <- match_idx[match_idx > 0]
  if (length(hit) == 0) {
    NA_character_
  } else {
    values[[hit[[1]]]]
  }
}

args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 1) {
  stop(
    paste(
      "Usage:",
      "Rscript import_postgres_parcels_to_lookup.R <output_sqlite> [schema.table] [address_column] [geoid_column] [chunk_size]",
      "",
      "Connection is read from env vars:",
      "PGHOST (default 127.0.0.1)",
      "PGPORT (default 5432)",
      "PGDATABASE (default postgres)",
      "PGUSER (default postgres)",
      "PGPASSWORD (optional)",
      sep = "\n"
    )
  )
}

output_sqlite <- args[[1]]
table_ref <- if (length(args) >= 2) args[[2]] else "public.parcels"
address_col_arg <- if (length(args) >= 3) args[[3]] else NA_character_
geoid_col_arg <- if (length(args) >= 4) args[[4]] else NA_character_
chunk_size <- if (length(args) >= 5) suppressWarnings(as.integer(args[[5]])) else 100000L
if (is.na(chunk_size) || chunk_size <= 0) {
  chunk_size <- 100000L
}

if (!grepl("\\.", table_ref)) {
  stop("table_ref must be in schema.table format, e.g. public.parcels")
}

schema_name <- strsplit(table_ref, "\\.", fixed = FALSE)[[1]][1]
table_name <- strsplit(table_ref, "\\.", fixed = FALSE)[[1]][2]

pg_con <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PGHOST", "127.0.0.1"),
  port = as.integer(Sys.getenv("PGPORT", "5432")),
  dbname = Sys.getenv("PGDATABASE", "postgres"),
  user = Sys.getenv("PGUSER", "postgres"),
  password = Sys.getenv("PGPASSWORD", "")
)
on.exit(dbDisconnect(pg_con), add = TRUE)

sqlite_con <- dbConnect(RSQLite::SQLite(), output_sqlite)
on.exit(dbDisconnect(sqlite_con), add = TRUE)

dbExecute(sqlite_con, "PRAGMA journal_mode=WAL;")
dbExecute(
  sqlite_con,
  "CREATE TABLE IF NOT EXISTS address_geoid_lookup (
    normalized_address TEXT PRIMARY KEY,
    address TEXT,
    geoid TEXT,
    source TEXT,
    updated_at TEXT
  )"
)
dbExecute(sqlite_con, "CREATE INDEX IF NOT EXISTS idx_address_geoid_lookup_geoid ON address_geoid_lookup(geoid)")
dbExecute(
  sqlite_con,
  "CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
  )"
)

dbExecute(sqlite_con, "CREATE TEMP TABLE IF NOT EXISTS incoming_chunk (
  normalized_address TEXT,
  address TEXT,
  geoid TEXT,
  source TEXT
)")

available_cols <- dbGetQuery(
  pg_con,
  "SELECT column_name
   FROM information_schema.columns
   WHERE table_schema = $1 AND table_name = $2
   ORDER BY ordinal_position",
  params = list(schema_name, table_name)
)$column_name

if (length(available_cols) == 0) {
  stop(paste0("No columns found for ", table_ref, ". Check table/schema name."))
}

address_col <- if (!is.na(address_col_arg)) {
  address_col_arg
} else {
  pick_first_existing(
    c("address", "site_address", "full_address", "property_address", "situs_address", "street_address", "loc_address"),
    available_cols
  )
}

geoid_col <- if (!is.na(geoid_col_arg)) {
  geoid_col_arg
} else {
  pick_first_existing(
    c("geoid", "tract_geoid", "census_geoid", "geoid10", "block_group_geoid"),
    available_cols
  )
}

if (is.na(address_col) || !(address_col %in% available_cols)) {
  stop(
    paste0(
      "Could not determine address column. Pass it explicitly as arg 3. Available columns: ",
      paste(available_cols, collapse = ", ")
    )
  )
}

if (is.na(geoid_col) || !(geoid_col %in% available_cols)) {
  stop(
    paste0(
      "Could not determine GEOID column. Pass it explicitly as arg 4. Available columns: ",
      paste(available_cols, collapse = ", ")
    )
  )
}

query_sql <- sprintf(
  "SELECT \"%s\" AS address, \"%s\" AS geoid FROM \"%s\".\"%s\" WHERE \"%s\" IS NOT NULL AND \"%s\" IS NOT NULL",
  address_col, geoid_col, schema_name, table_name, address_col, geoid_col
)

rs <- dbSendQuery(pg_con, query_sql)
on.exit(dbClearResult(rs), add = TRUE)

insert_sql <- "INSERT INTO address_geoid_lookup(normalized_address, address, geoid, source, updated_at)
               SELECT normalized_address, address, geoid, source, datetime('now')
               FROM (
                 SELECT normalized_address, address, geoid, source
                 FROM incoming_chunk
                 WHERE normalized_address IS NOT NULL
                   AND normalized_address <> ''
                   AND geoid IS NOT NULL
                   AND geoid <> ''
                 GROUP BY normalized_address
               )
               ON CONFLICT(normalized_address)
               DO UPDATE SET
                 address = excluded.address,
                 geoid = excluded.geoid,
                 source = excluded.source,
                 updated_at = datetime('now')"

total_read <- 0L
total_upserted <- 0L

repeat {
  chunk <- dbFetch(rs, n = chunk_size)
  if (!is.data.frame(chunk) || nrow(chunk) == 0) {
    break
  }

  total_read <- total_read + nrow(chunk)

  cleaned <- chunk %>%
    transmute(
      address = as.character(address),
      normalized_address = normalize_address(address),
      geoid = normalize_geoid(geoid),
      source = "postgres_parcels"
    ) %>%
    filter(!is.na(address), nzchar(address), !is.na(normalized_address), nzchar(normalized_address), !is.na(geoid), nzchar(geoid))

  if (nrow(cleaned) > 0) {
    dbWithTransaction(sqlite_con, {
      dbExecute(sqlite_con, "DELETE FROM incoming_chunk")
      dbWriteTable(sqlite_con, "incoming_chunk", cleaned, append = TRUE)
      dbExecute(sqlite_con, insert_sql)
    })

    total_upserted <- total_upserted + nrow(cleaned)
  }
}

final_count <- dbGetQuery(sqlite_con, "SELECT COUNT(*) AS n FROM address_geoid_lookup")$n[[1]]

dbExecute(sqlite_con, "INSERT OR REPLACE INTO metadata(key, value) VALUES ('updated_at_utc', datetime('now'))")
dbExecute(sqlite_con, "INSERT OR REPLACE INTO metadata(key, value) VALUES ('row_count', ?)", params = list(as.character(final_count)))
dbExecute(sqlite_con, "INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_import_source', 'postgres_parcels')")
dbExecute(sqlite_con, "INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_import_table', ?)", params = list(table_ref))

cat(sprintf("Imported from %s\n", table_ref))
cat(sprintf("Rows read from Postgres: %d\n", total_read))
cat(sprintf("Rows cleaned/upserted: %d\n", total_upserted))
cat(sprintf("Total cached lookup rows: %d\n", final_count))
