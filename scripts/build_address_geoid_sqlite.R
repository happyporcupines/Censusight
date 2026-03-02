# -----------------------------------------------------------------------------
# File: scripts/build_address_geoid_sqlite.R
# Purpose: Build and maintain a local SQLite address-to-GEOID lookup database.
# Responsibilities:
# - Initialize lookup/metadata tables in SQLite
# - Ingest optional CSV address data and normalize records
# - Seed lookup rows from provided GEOIDs when available
# - Geocode unmatched addresses through the Census geocoder (bounded per run)
# - draws from personally made database of wisconsin addresses and geocodes to create a local cache for the app
# - Persist metadata about the lookup state for auditing and troubleshooting
# -----------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(httr)
})

normalize_address <- function(x) {
  x <- tolower(trimws(x))
  x <- gsub("[[:punct:]]", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 1) {
  stop(
    paste(
      "Usage:",
      "Rscript build_address_geoid_sqlite.R <output_sqlite> [input_csv] [max_geocode]",
      "\\n- output_sqlite: path to SQLite lookup DB",
      "\\n- input_csv (optional): CSV with an 'address' column; optional geoid column",
      "\\n- max_geocode (optional): max unmatched addresses to geocode this run (default 5000)",
      sep = "\n"
    )
  )
}

output_sqlite <- args[[1]]
input_csv <- if (length(args) >= 2) args[[2]] else NA_character_
max_geocode <- if (length(args) >= 3) suppressWarnings(as.integer(args[[3]])) else 5000L

if (is.na(max_geocode) || max_geocode <= 0) {
  max_geocode <- 5000L
}

normalize_geoid <- function(x) {
  x <- gsub("[^0-9]", "", as.character(x))
  ifelse(nchar(x) >= 11, x, NA_character_)
}

ensure_wisconsin_hint <- function(address) {
  address <- as.character(address)
  lower_address <- tolower(address)
  if (grepl("\\b(wi|wisconsin)\\b", lower_address)) {
    return(address)
  }
  paste0(address, ", Wisconsin")
}

geocode_address_to_geoid <- function(address) {
  safe_address <- ensure_wisconsin_hint(address)

  response <- tryCatch(
    {
      GET(
        url = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress",
        query = list(
          address = safe_address,
          benchmark = "Public_AR_Current",
          vintage = "Current_Current",
          layers = "all",
          format = "json"
        ),
        timeout(30)
      )
    },
    error = function(e) NULL
  )

  if (is.null(response) || status_code(response) >= 300) {
    return(NA_character_)
  }

  parsed <- tryCatch(content(response, as = "parsed", type = "application/json"), error = function(e) NULL)
  if (is.null(parsed)) {
    return(NA_character_)
  }

  matches <- parsed$result$addressMatches
  if (is.null(matches) || length(matches) == 0) {
    return(NA_character_)
  }

  geographies <- matches[[1]]$geographies
  if (is.null(geographies)) {
    return(NA_character_)
  }

  tract_geo <- geographies[["Census Tracts"]]
  if (is.null(tract_geo) || length(tract_geo) == 0 || is.null(tract_geo[[1]]$GEOID)) {
    return(NA_character_)
  }

  normalize_geoid(tract_geo[[1]]$GEOID)
}

con <- dbConnect(SQLite(), output_sqlite)
on.exit(dbDisconnect(con), add = TRUE)

dbExecute(con, "PRAGMA journal_mode=WAL;")
dbExecute(
  con,
  "CREATE TABLE IF NOT EXISTS address_geoid_lookup (
    normalized_address TEXT PRIMARY KEY,
    address TEXT,
    geoid TEXT,
    source TEXT,
    updated_at TEXT
  )"
)
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_address_geoid_lookup_geoid ON address_geoid_lookup(geoid)")

dbExecute(
  con,
  "CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
  )"
)
dbExecute(con, "INSERT OR REPLACE INTO metadata(key, value) VALUES ('created_at_utc', datetime('now'))")

if (!is.na(input_csv) && !file.exists(input_csv)) {
  stop(paste("Input CSV not found:", input_csv))
}

if (is.na(input_csv)) {
  row_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM address_geoid_lookup")$n[[1]]
  dbExecute(con, "INSERT OR REPLACE INTO metadata(key, value) VALUES ('row_count', ?)", params = list(as.character(row_count)))
  cat(sprintf("Initialized lookup DB at %s. Current cached rows: %d\n", output_sqlite, row_count))
  quit(save = "no", status = 0)
}

raw_df <- read.csv(input_csv, stringsAsFactors = FALSE, check.names = FALSE)
names(raw_df) <- tolower(names(raw_df))

if (!"address" %in% names(raw_df)) {
  stop("Input CSV must include an 'address' column.")
}

geoid_col <- if ("geoid" %in% names(raw_df)) {
  "geoid"
} else {
  alt <- names(raw_df)[names(raw_df) %in% c("tract_geoid", "census_geoid", "block_group_geoid")]
  if (length(alt) > 0) alt[[1]] else NA_character_
}

lookup_df <- raw_df %>%
  transmute(address = as.character(address)) %>%
  mutate(
    normalized_address = normalize_address(address),
    geoid = if (!is.na(geoid_col)) as.character(.data[[geoid_col]]) else NA_character_
  ) %>%
  filter(!is.na(address), nzchar(address), !is.na(normalized_address), nzchar(normalized_address)) %>%
  mutate(geoid = normalize_geoid(geoid)) %>%
  group_by(normalized_address) %>%
  summarise(
    address = dplyr::first(address),
    geoid = dplyr::first(geoid[!is.na(geoid)], default = NA_character_),
    .groups = "drop"
  )

if (nrow(lookup_df) == 0) {
  stop("No valid addresses were found after cleaning.")
}

seed_rows <- lookup_df %>%
  filter(!is.na(geoid), nzchar(geoid)) %>%
  mutate(source = "input_geoid")

if (nrow(seed_rows) > 0) {
  sql <- "INSERT OR REPLACE INTO address_geoid_lookup(normalized_address, address, geoid, source, updated_at)
          VALUES (?, ?, ?, ?, datetime('now'))"

  for (i in seq_len(nrow(seed_rows))) {
    dbExecute(
      con,
      sql,
      params = list(
        seed_rows$normalized_address[[i]],
        seed_rows$address[[i]],
        seed_rows$geoid[[i]],
        seed_rows$source[[i]]
      )
    )
  }
}

needs_geocode <- lookup_df %>%
  filter(is.na(geoid) | !nzchar(geoid)) %>%
  slice_head(n = max_geocode)

geocoded_ok <- 0L
if (nrow(needs_geocode) > 0) {
  geocode_rows <- needs_geocode
  geocode_rows$geoid <- vapply(geocode_rows$address, geocode_address_to_geoid, FUN.VALUE = character(1))
  geocode_rows <- geocode_rows %>%
    filter(!is.na(geoid), nzchar(geoid)) %>%
    mutate(source = "census_geocoder")

  geocoded_ok <- nrow(geocode_rows)

  if (geocoded_ok > 0) {
    sql <- "INSERT OR REPLACE INTO address_geoid_lookup(normalized_address, address, geoid, source, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))"

    for (i in seq_len(geocoded_ok)) {
      dbExecute(
        con,
        sql,
        params = list(
          geocode_rows$normalized_address[[i]],
          geocode_rows$address[[i]],
          geocode_rows$geoid[[i]],
          geocode_rows$source[[i]]
        )
      )
    }
  }
}

row_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM address_geoid_lookup")$n[[1]]
dbExecute(con, "INSERT OR REPLACE INTO metadata(key, value) VALUES ('row_count', ?)", params = list(as.character(row_count)))
dbExecute(con, "INSERT OR REPLACE INTO metadata(key, value) VALUES ('updated_at_utc', datetime('now'))")

cat(
  sprintf(
    paste0(
      "Lookup DB ready at %s\n",
      "Input unique addresses: %d\n",
      "Rows with GEOID from input: %d\n",
      "Rows geocoded this run: %d\n",
      "Total cached rows now: %d\n"
    ),
    output_sqlite,
    nrow(lookup_df),
    nrow(seed_rows),
    geocoded_ok,
    row_count
  )
)
