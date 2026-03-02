# -----------------------------------------------------------------------------
# File: scripts/import_postgres_parcels_to_lookup_psql.R
# Purpose: Import parcel address/GEOID records from Postgres using `psql` CLI.
# Responsibilities:
# - Execute Postgres queries through shell-safe `psql` calls
# - Pull records in chunks and normalize address/GEOID fields
# - Upsert cleaned results into the SQLite lookup cache
# - Track import metadata and provide run summaries
# - brings in the data for build_address_geoid_sqlite.R from a postgres database instead of a csv
# -----------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(dplyr)
})

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

quote_ident <- function(x) {
  paste0('"', gsub('"', '""', x), '"')
}

quote_literal <- function(x) {
  paste0("'", gsub("'", "''", x), "'")
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
      "Rscript import_postgres_parcels_to_lookup_psql.R <output_sqlite> [schema.table] [address_column] [geoid_column] [chunk_size] [max_rows]",
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

if (!nzchar(Sys.which("psql"))) {
  stop("psql is required for this importer but was not found in PATH.")
}

output_sqlite <- args[[1]]
table_ref <- if (length(args) >= 2) args[[2]] else "public.parcels"
address_col_arg <- if (length(args) >= 3) args[[3]] else NA_character_
geoid_col_arg <- if (length(args) >= 4) args[[4]] else NA_character_
chunk_size <- if (length(args) >= 5) suppressWarnings(as.integer(args[[5]])) else 50000L
max_rows <- if (length(args) >= 6) suppressWarnings(as.integer(args[[6]])) else NA_integer_

if (is.na(chunk_size) || chunk_size <= 0) {
  chunk_size <- 50000L
}

if (!is.na(address_col_arg) && (!nzchar(trimws(address_col_arg)) || tolower(trimws(address_col_arg)) == "na")) {
  address_col_arg <- NA_character_
}

if (!is.na(geoid_col_arg) && (!nzchar(trimws(geoid_col_arg)) || tolower(trimws(geoid_col_arg)) == "na")) {
  geoid_col_arg <- NA_character_
}

if (!is.na(max_rows) && max_rows <= 0) {
  max_rows <- NA_integer_
}

if (!grepl("\\.", table_ref)) {
  stop("table_ref must be in schema.table format, e.g. public.parcels")
}

table_ref_clean <- gsub("\\s+", "", trimws(table_ref))

schema_name <- strsplit(table_ref_clean, "\\.", fixed = FALSE)[[1]][1]
table_name <- strsplit(table_ref_clean, "\\.", fixed = FALSE)[[1]][2]

pg_host <- Sys.getenv("PGHOST", "127.0.0.1")
pg_port <- Sys.getenv("PGPORT", "5432")
pg_db <- Sys.getenv("PGDATABASE", "postgres")
pg_user <- Sys.getenv("PGUSER", "postgres")
pg_password <- Sys.getenv("PGPASSWORD", "")

psql_base_args <- c(
  "-X", "-q",
  "-h", pg_host,
  "-p", pg_port,
  "-U", pg_user,
  "-d", pg_db,
  "-v", "ON_ERROR_STOP=1"
)

run_psql <- function(sql, tuples_only = FALSE, unaligned = FALSE) {
  sql <- gsub("[\r\n]+", " ", sql, perl = TRUE)
  sql <- gsub("[[:space:]]+", " ", sql, perl = TRUE)
  sql <- trimws(sql)

  call_args <- psql_base_args
  if (tuples_only) {
    call_args <- c(call_args, "-t")
  }
  if (unaligned) {
    call_args <- c(call_args, "-A")
  }
  call_args <- c(call_args, "-c", sql)

  psql_bin <- Sys.which("psql")
  if (!nzchar(psql_bin)) {
    stop("psql is required for this importer but was not found in PATH.")
  }

  cmd <- paste(
    shQuote(psql_bin),
    paste(vapply(call_args, shQuote, FUN.VALUE = character(1)), collapse = " "),
    "2>&1"
  )

  if (nzchar(pg_password)) {
    cmd <- paste(paste0("PGPASSWORD=", shQuote(pg_password)), cmd)
  }

  out <- system(cmd, intern = TRUE)
  status <- attr(out, "status")
  if (!is.null(status) && status != 0) {
    stop(paste(out, collapse = "\n"))
  }
  out
}

run_psql_copy_csv <- function(select_sql) {
  copy_cmd <- sprintf("COPY (%s) TO STDOUT WITH CSV HEADER", select_sql)
  out <- run_psql(copy_cmd)
  if (length(out) == 0) {
    return(data.frame(stringsAsFactors = FALSE))
  }

  csv_text <- paste(out, collapse = "\n")
  if (!nzchar(trimws(csv_text))) {
    return(data.frame(stringsAsFactors = FALSE))
  }

  read.csv(text = csv_text, stringsAsFactors = FALSE, check.names = FALSE)
}

sqlite_con <- dbConnect(RSQLite::SQLite(), output_sqlite)
on.exit(dbDisconnect(sqlite_con), add = TRUE)

exec_sqlite <- function(sql, params = NULL) {
  if (is.null(params)) {
    dbExecute(sqlite_con, sql)
  } else {
    dbExecute(sqlite_con, sql, params = params)
  }
  invisible(NULL)
}

exec_sqlite("PRAGMA journal_mode=WAL;")
exec_sqlite(
  "CREATE TABLE IF NOT EXISTS address_geoid_lookup (
    normalized_address TEXT PRIMARY KEY,
    address TEXT,
    geoid TEXT,
    source TEXT,
    updated_at TEXT
  )"
)
exec_sqlite("CREATE INDEX IF NOT EXISTS idx_address_geoid_lookup_geoid ON address_geoid_lookup(geoid)")
exec_sqlite(
  "CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
  )"
)
exec_sqlite(
  "CREATE TEMP TABLE IF NOT EXISTS incoming_chunk (
    normalized_address TEXT,
    address TEXT,
    geoid TEXT,
    source TEXT
  )"
)

header_probe_sql <- sprintf("SELECT * FROM %s LIMIT 0", table_ref_clean)
header_probe <- run_psql_copy_csv(header_probe_sql)
available_cols <- names(header_probe)

if (length(available_cols) == 0) {
  stop(paste0("No columns found for ", table_ref_clean, ". Check table/schema name and credentials."))
}

address_col <- if (!is.na(address_col_arg)) {
  address_col_arg
} else {
  pick_first_existing(
    c("address", "site_address", "siteadress", "pstladress", "full_address", "property_address", "situs_address", "street_address", "loc_address"),
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

addr_ident <- quote_ident(address_col)
use_derived_stateid <- FALSE

if (!is.na(geoid_col) && geoid_col %in% available_cols) {
  geoid_expr <- quote_ident(geoid_col)
  geoid_nonnull_pred <- sprintf("%s IS NOT NULL", geoid_expr)
  geoid_source_desc <- geoid_col
} else {
  stateid_col <- pick_first_existing(c("stateid"), available_cols)

  if (is.na(stateid_col) || !(stateid_col %in% available_cols)) {
    stop(
      paste0(
        "Could not determine GEOID-equivalent column. Pass GEOID column explicitly as arg 4, or provide a table with stateid. Available columns: ",
        paste(available_cols, collapse = ", ")
      )
    )
  }

  stateid_ident <- quote_ident(stateid_col)
  geoid_expr <- sprintf("'55' || substr(regexp_replace(%s::text, '[^0-9]', '', 'g'), 1, 9)", stateid_ident)
  geoid_nonnull_pred <- sprintf("length(regexp_replace(%s::text, '[^0-9]', '', 'g')) >= 9", stateid_ident)
  geoid_source_desc <- "derived_from_stateid"
  use_derived_stateid <- TRUE
}

cursor_col <- pick_first_existing(c("objectid", "id", "gid", "ogc_fid"), available_cols)
use_keyset <- !is.na(cursor_col) && cursor_col %in% available_cols
cursor_ident <- if (use_keyset) quote_ident(cursor_col) else NA_character_

insert_sql <- "INSERT OR REPLACE INTO address_geoid_lookup(normalized_address, address, geoid, source, updated_at)
               SELECT normalized_address, address, geoid, source, datetime('now')
               FROM (
                 SELECT normalized_address, address, geoid, source
                 FROM incoming_chunk
                 WHERE normalized_address IS NOT NULL
                   AND normalized_address <> ''
                   AND geoid IS NOT NULL
                   AND geoid <> ''
                 GROUP BY normalized_address
               )"

total_read <- 0L
total_upserted <- 0L
offset <- 0L
last_cursor <- NULL
rows_remaining <- max_rows

repeat {
  this_limit <- if (is.na(rows_remaining)) {
    chunk_size
  } else {
    as.integer(min(chunk_size, rows_remaining))
  }

  if (this_limit <= 0) {
    break
  }

  base_where <- sprintf("%s IS NOT NULL AND %s", addr_ident, geoid_nonnull_pred)

  if (use_keyset && !is.null(last_cursor)) {
    base_where <- sprintf("%s AND %s > %s", base_where, cursor_ident, quote_literal(last_cursor))
  }

  if (use_keyset) {
    select_sql <- sprintf(
      "SELECT %s AS __cursor_key, %s AS address, %s AS geoid
       FROM %s
       WHERE %s
       ORDER BY %s
       LIMIT %d",
      cursor_ident,
      addr_ident,
      geoid_expr,
      table_ref_clean,
      base_where,
      cursor_ident,
      this_limit
    )
  } else {
    select_sql <- sprintf(
      "SELECT %s AS address, %s AS geoid
       FROM %s
       WHERE %s
       OFFSET %d LIMIT %d",
      addr_ident,
      geoid_expr,
      table_ref_clean,
      base_where,
      offset,
      this_limit
    )
  }

  chunk <- run_psql_copy_csv(select_sql)

  if (!is.data.frame(chunk) || nrow(chunk) == 0) {
    break
  }

  if (use_keyset) {
    last_cursor <- as.character(chunk[["__cursor_key"]][[nrow(chunk)]])
    chunk <- chunk %>% select(-all_of("__cursor_key"))
  } else {
    offset <- offset + nrow(chunk)
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
      exec_sqlite("DELETE FROM incoming_chunk")
      dbWriteTable(sqlite_con, "incoming_chunk", cleaned, append = TRUE)
      exec_sqlite(insert_sql)
    })

    total_upserted <- total_upserted + nrow(cleaned)
  }

  if (!is.na(rows_remaining)) {
    rows_remaining <- rows_remaining - nrow(chunk)
  }

  cat(sprintf("Processed %d source row(s)...\n", total_read))

  if (nrow(chunk) < this_limit) {
    break
  }
}

final_count <- dbGetQuery(sqlite_con, "SELECT COUNT(*) AS n FROM address_geoid_lookup")$n[[1]]

exec_sqlite("INSERT OR REPLACE INTO metadata(key, value) VALUES ('updated_at_utc', datetime('now'))")
exec_sqlite("INSERT OR REPLACE INTO metadata(key, value) VALUES ('row_count', ?)", params = list(as.character(final_count)))
exec_sqlite("INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_import_source', 'postgres_parcels_psql')")
exec_sqlite("INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_import_table', ?)", params = list(table_ref_clean))
exec_sqlite("INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_import_geoid_source', ?)", params = list(geoid_source_desc))
exec_sqlite("INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_import_rows_read', ?)", params = list(as.character(total_read)))
exec_sqlite("INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_import_rows_upserted', ?)", params = list(as.character(total_upserted)))
exec_sqlite("INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_import_paging', ?)", params = list(if (use_keyset) paste0('keyset:', cursor_col) else 'offset'))

cat(sprintf("Imported from %s\n", table_ref_clean))
cat(sprintf("Address column used: %s\n", address_col))
cat(sprintf("GEOID source used: %s\n", geoid_source_desc))
if (use_derived_stateid) {
  cat("Derived tract GEOID rule: '55' + first 9 digits of stateid (after stripping non-digits).\n")
}
cat(sprintf("Rows read from Postgres: %d\n", total_read))
cat(sprintf("Rows cleaned/upserted: %d\n", total_upserted))
cat(sprintf("Total cached lookup rows: %d\n", final_count))
