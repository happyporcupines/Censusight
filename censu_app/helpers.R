# -----------------------------------------------------------------------------
# File: scripts/R/helpers.R
# Purpose: Shared utility functions for Censusight data prep and Census joins.
# Responsibilities:
# - Clean and validate uploaded address records
# - Normalize dataset names, vintage years, and geography tokens
# - Manage cached Census catalog/variable metadata and lookup workflows
# - Provide reusable join/sanitization helpers consumed by server logic
# -----------------------------------------------------------------------------

# --- 1. Helper Logic ---
# This file centralizes reusable, non-reactive logic that server/UI call into.
# Groupings below are organized by workflow stage to make maintenance easier.

# --- 1.1 Address Cleaning & Validation Helpers ---

# Validates expected upload columns, normalizes address/ZIP fields,
# attempts local GEOID lookup matching, and partitions good/bad records.

clean_addresses <- function(df) {

  # Enforce required schema early so downstream joins do not silently fail.

  if (!"address" %in% colnames(df)) {
    stop("Uh oh! The uploaded CSV doesn't have a column named 'address' (all lowercase).")
  }

  # Standardize address and ZIP text before lookup matching.
  df$address <- toupper(trimws(gsub("\\s+", " ", as.character(df$address))))
  df$cleaned_address <- normalize_address_text(df$address)

  zip_col <- names(df)[tolower(names(df)) %in% c("zip", "zipcode", "zip_code")]
  has_zip <- length(zip_col) >= 1

  # Normalize ZIP to 5 numeric digits when present so later ZIP/ZCTA joins
  # can still use valid rows, but do not reject successful parcel matches for
  # bad ZIP values alone.
  if (has_zip) {
    zip_values <- as.character(df[[zip_col[1]]])
    zip_digits <- substr(gsub("[^0-9]", "", zip_values), 1, 5)
    valid_zip <- !is.na(zip_digits) & nchar(zip_digits) == 5
    df[[zip_col[1]]] <- ifelse(valid_zip, zip_digits, NA_character_)
  }

  lookup_db_path <- censusight_data_path("address_geoid.sqlite", must_exist = TRUE)
  lookup_db_ok <- file.exists(lookup_db_path)

  # Use the same local lookup path as the Census join so upload processing,
  # parcel checks, and the join workflow classify addresses the same way.
  if (lookup_db_ok) {
    enrichment <- attach_geoid_from_lookup(
      df,
      lookup_db_path,
      geocode_limit = 0
    )
    df <- enrichment$data
  } else if (!"geoid" %in% names(df)) {
    df$geoid <- NA_character_
  }

  # Normalize case-insensitive GEOID output from the shared enrichment path.
  geoid_col <- names(df)[tolower(names(df)) == "geoid"]
  if (length(geoid_col) >= 1) {
    df$geoid <- as.character(df[[geoid_col[1]]])
  } else {
    df$geoid <- NA_character_
  }

  df$geoid <- ifelse(
    is.na(df$geoid),
    NA_character_,
    gsub("[^0-9]", "", as.character(df$geoid))
  )
  address_match <- !is.na(df$geoid) & nchar(df$geoid) >= 11
  has_clean_address <- !is.na(df$cleaned_address) & nzchar(df$cleaned_address)
  good_idx <- address_match & has_clean_address

  # Split output into valid/invalid sets consumed by process/join workflows.
  matches <- df[good_idx, , drop = FALSE]
  errors <- df[!good_idx, , drop = FALSE]

  # Attach user-facing failure reason for each invalid row.
  if (nrow(errors) > 0) {
    errors$error_reason <- ifelse(
      !has_clean_address[!good_idx],
      "Missing or empty address",
      ifelse(
        !lookup_db_ok,
        paste0("Lookup database not found: ", lookup_db_path),
        "Address did not match parcel GEOID lookup"
      )
    )
  }

  list(
    good_data = matches,
    bad_data = errors
  )
}

# Converts arbitrary address text into a normalized tokenized form used
# for deterministic lookup keys and join pre-cleaning.
normalize_address_text <- function(x) {
  x <- tolower(trimws(as.character(x)))
  x <- gsub("[[:punct:]]", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# Produces a canonical address variant by standardizing suffixes,
# directional words, and unit/floor fragments.
canonicalize_address_text <- function(x) {
  x <- normalize_address_text(x)
  x <- gsub("\\b(apartment|apt|suite|ste|unit|floor|fl|room|rm)\\b.*$", "", x)
  x <- gsub("\\b(north)\\b", "n", x)
  x <- gsub("\\b(south)\\b", "s", x)
  x <- gsub("\\b(east)\\b", "e", x)
  x <- gsub("\\b(west)\\b", "w", x)
  x <- gsub("\\b(street)\\b", "st", x)
  x <- gsub("\\b(avenue)\\b", "ave", x)
  x <- gsub("\\b(road)\\b", "rd", x)
  x <- gsub("\\b(drive)\\b", "dr", x)
  x <- gsub("\\b(lane)\\b", "ln", x)
  x <- gsub("\\b(court)\\b", "ct", x)
  x <- gsub("\\b(place)\\b", "pl", x)
  x <- gsub("\\b(boulevard)\\b", "blvd", x)
  x <- gsub("\\b(terrace)\\b", "ter", x)
  x <- gsub("\\b(trail)\\b", "trl", x)
  x <- gsub("\\b(parkway)\\b", "pkwy", x)
  x <- gsub("\\b(circle)\\b", "cir", x)
  x <- gsub("\\b(highway)\\b", "hwy", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# Returns multiple normalized variants to improve local lookup hit rates.
address_lookup_variants <- function(x) {
  original <- normalize_address_text(x)
  canonical <- canonicalize_address_text(x)
  no_dir <- gsub("^([0-9a-z]+)\\s+(n|s|e|w|ne|nw|se|sw)\\s+", "\\1 ", canonical)
  unique(c(original, canonical, no_dir))
}

# --- 1.2 Dataset/Geography Normalization Helpers ---
# These helpers normalize catalog and variable metadata so server-side filters
# can consistently compare datasets, years, and geography capabilities.

# Maps catalog/API dataset identifiers to tidycensus-compatible dataset codes.
normalize_dataset_name <- function(name) {
  name <- tolower(trimws(as.character(name)))
  name[is.na(name)] <- ""
  name <- gsub("^https?://api\\.census\\.gov/data/[0-9]{4}/", "", name)

  case_when(
    name %in% c("acs1", "acs3", "acs5", "acsse", "pl", "dhc", "sf1", "sf3") ~ name,
    grepl("(^|/)acs/acs1$", name) ~ "acs1",
    grepl("(^|/)acs/acs3$", name) ~ "acs3",
    grepl("(^|/)acs/acs5$", name) ~ "acs5",
    grepl("(^|/)acs/acsse$", name) ~ "acsse",
    grepl("(^|/)dec/pl$", name) ~ "pl",
    grepl("(^|/)dec/dhc$", name) ~ "dhc",
    grepl("(^|/)dec/sf1$", name) ~ "sf1",
    grepl("(^|/)dec/sf3$", name) ~ "sf3",
    TRUE ~ NA_character_
  )
}

# Extracts a four-digit year from flexible vintage representations.
parse_vintage_year <- function(x) {
  raw <- as.character(x)
  raw[is.na(raw)] <- ""

  parsed <- suppressWarnings(as.integer(raw))
  needs_extract <- is.na(parsed) & nzchar(raw)

  if (any(needs_extract)) {
    extracted <- sub(".*?([0-9]{4}).*", "\\1", raw[needs_extract])
    parsed[needs_extract] <- suppressWarnings(as.integer(extracted))
  }

  parsed
}

# Returns documented geography-level availability text per dataset,
# used in the dataset selector label and join-geography validation.
dataset_geography_levels <- function(dataset_name) {
  ds <- normalize_dataset_name(dataset_name)

  # Default: state-only (conservative fallback)
  out <- rep("state", length(ds))

  # ACS 5-Year: finest geography — down to block group and ZCTA.
  out[!is.na(ds) & ds == "acs5"] <-
    "state, county, tract, block group, zcta"

  # ACS 1-Year / 3-Year / Supplemental: only state and county.
  out[!is.na(ds) & ds %in% c("acs1", "acs3", "acsse")] <-
    "state, county"

  # Decennial PL / DHC / SF1: tract and block group available.
  out[!is.na(ds) & ds %in% c("pl", "dhc", "sf1")] <-
    "state, county, tract, block group"

  # Decennial SF3: only down to tract (block group not in tidycensus for SF3).
  out[!is.na(ds) & ds == "sf3"] <-
    "state, county, tract"

  out
}

# Formats variable geography metadata into concise chooser labels.
format_variable_geography <- function(geography_text) {
  if (is.null(geography_text) ||
      is.na(geography_text) ||
      !nzchar(trimws(geography_text))) {
    return("levels: unknown")
  }

  geo_values <- unique(trimws(unlist(strsplit(as.character(geography_text), ","))))
  geo_values <- geo_values[nzchar(geo_values)]

  if (length(geo_values) == 0) {
    return("levels: unknown")
  }

  geo_values <- tolower(geo_values)
  geo_values <- gsub("\\s+", " ", geo_values)

  if (length(geo_values) > 5) {
    geo_values <- c(geo_values[1:5], "...")
  }

  paste0("levels: ", paste(geo_values, collapse = ", "))
}

# Normalizes geography labels to canonical tokens
# used by availability checks.
normalize_geography_token <- function(x) {
  token <- tolower(trimws(as.character(x)))
  token <- gsub("\\s+", " ", token)

  if (token %in% c(
    "zip code tabulation area",
    "zip code tabulation areas",
    "zip",
    "zipcode"
  )) {
    return("zcta")
  }
  if (token %in% c("tract", "census tract", "census tracts")) {
    return("tract")
  }
  if (token %in% c("county", "counties")) {
    return("county")
  }
  if (token %in% c("state", "states")) {
    return("state")
  }

  token
}

# Splits and normalizes geography metadata into unique comparable tokens.
extract_geography_tokens <- function(geography_text) {
  if (is.null(geography_text) ||
      is.na(geography_text) ||
      !nzchar(trimws(geography_text))) {
    return(character(0))
  }

  tokens <- unique(trimws(unlist(strsplit(as.character(geography_text), ","))))
  tokens <- tokens[nzchar(tokens)]
  vapply(tokens, normalize_geography_token, FUN.VALUE = character(1))
}

# Returns TRUE when a variable can be queried for the target geography.
# Variables with missing/unknown geography metadata are only allowed at state
# level; sub-state joins require explicit confirmation to prevent empty results.
variable_available_for_geography <- function(geography_text, target_geography) {
  target <- normalize_geography_token(target_geography)
  tokens <- extract_geography_tokens(geography_text)

  if (length(tokens) == 0) {
    return(identical(target, "state"))
  }

  target %in% tokens
}

# Fills in the geography column for variables that have NA/empty geography,
# using documented dataset-level coverage as a conservative default.
# This removes "levels: unknown" from the UI and enables accurate filtering.
fill_dataset_geography <- function(vars_df, dataset_name) {
  if (!is.data.frame(vars_df) || nrow(vars_df) == 0) {
    return(vars_df)
  }

  ds <- normalize_dataset_name(dataset_name)

  default_geo <- switch(
    ds,
    "acs5"  = "state, county, tract, block group, zcta",
    "acs1"  = "state, county",
    "acs3"  = "state, county",
    "acsse" = "state, county",
    "pl"    = "state, county, tract, block group",
    "dhc"   = "state, county, tract, block group",
    "sf1"   = "state, county, tract, block group",
    "sf3"   = "state, county, tract",
    "state"
  )

  miss <- is.na(vars_df$geography) | !nzchar(trimws(as.character(vars_df$geography)))
  vars_df$geography[miss] <- default_geo
  vars_df
}

# Detects Puerto Rico-specific variables based on label/concept text.
is_puerto_rico_only_variable <- function(label_text, concept_text) {
  text_blob <- paste(as.character(label_text), as.character(concept_text), collapse = " ")
  grepl("puerto rico", text_blob, ignore.case = TRUE)
}

# State-specific gatekeeper used to exclude incompatible variables.
variable_allowed_for_state <- function(label_text, concept_text, state_abbr = "WI") {
  state_norm <- toupper(trimws(as.character(state_abbr)))

  if (!nzchar(state_norm) || identical(state_norm, "PR")) {
    return(TRUE)
  }

  !is_puerto_rico_only_variable(label_text, concept_text)
}

# Ensures required variable metadata columns exist and are character typed.
ensure_variable_columns <- function(vars_df) {
  if (!is.data.frame(vars_df)) {
    return(data.frame(
      name = character(0),
      label = character(0),
      concept = character(0),
      geography = character(0),
      stringsAsFactors = FALSE
    ))
  }

  if (!"name" %in% names(vars_df)) vars_df$name <- NA_character_
  if (!"label" %in% names(vars_df)) vars_df$label <- NA_character_
  if (!"concept" %in% names(vars_df)) vars_df$concept <- NA_character_
  if (!"geography" %in% names(vars_df)) vars_df$geography <- NA_character_

  vars_df %>%
    mutate(
      name = as.character(.data$name),
      label = as.character(.data$label),
      concept = as.character(.data$concept),
      geography = as.character(.data$geography)
    )
}

# Cleans metadata labels into deterministic snake_case column-safe tokens.
sanitize_census_header <- function(x) {
  x <- tolower(trimws(as.character(x)))
  x <- gsub("!!", " ", x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  x
}

# --- 1.3 Lookup Database & Geocoding Helpers ---
# The functions below maintain a local address->GEOID cache and optionally
# backfill unresolved addresses via Census geocoder calls.

# Ensures lookup schema/indexes exist before reads/writes.
ensure_address_lookup_db <- function(db_path) {
  # Create DB objects idempotently so callers can safely run this each time.
  con <- dbConnect(RSQLite::SQLite(), db_path)
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
  dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_address_geoid_lookup_geoid ON address_geoid_lookup(geoid)"
  )
  dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT
      )"
  )
}

# Appends a Wisconsin hint when the address text omits state context.
ensure_wisconsin_hint <- function(address) {
  address <- as.character(address)
  lower_address <- tolower(address)
  if (grepl("\\b(wi|wisconsin)\\b", lower_address)) {
    return(address)
  }
  paste0(address, ", Wisconsin")
}

# Uses the Census geocoder endpoint to resolve a tract GEOID.
geocode_address_to_geoid <- function(address) {
  # Bias geocoding toward Wisconsin since app workflows are WI-focused.
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

  parsed <- tryCatch(
    content(response, as = "parsed", type = "application/json"),
    error = function(e) NULL
  )
  if (is.null(parsed)) {
    return(NA_character_)
  }

  matches <- parsed$result$addressMatches
  if (is.null(matches) || length(matches) == 0) {
    return(NA_character_)
  }

  geo <- matches[[1]]$geographies
  if (is.null(geo)) {
    return(NA_character_)
  }

  # Return the first tract-level GEOID if present; otherwise NA.
  tract_geo <- geo[["Census Tracts"]]
  if (!is.null(tract_geo) && length(tract_geo) > 0 && !is.null(tract_geo[[1]]$GEOID)) {
    geoid <- gsub("[^0-9]", "", as.character(tract_geo[[1]]$GEOID))
    if (nzchar(geoid) && nchar(geoid) >= 11) {
      return(geoid)
    }
  }

  NA_character_
}

# Bulk-fetches known GEOIDs for normalized address keys in chunks.
fetch_lookup_rows <- function(con, normalized_addresses) {
  # De-duplicate and remove blank keys before querying lookup table.
  normalized_addresses <- unique(
    normalized_addresses[!is.na(normalized_addresses) & nzchar(normalized_addresses)]
  )
  if (length(normalized_addresses) == 0) {
    return(data.frame(
      normalized_address = character(0),
      geoid = character(0),
      stringsAsFactors = FALSE
    ))
  }

  chunks <- split(normalized_addresses, ceiling(seq_along(normalized_addresses) / 900))
  out <- lapply(chunks, function(chunk) {
    placeholders <- paste(rep("?", length(chunk)), collapse = ",")
    sql <- paste0(
      "SELECT normalized_address, geoid FROM address_geoid_lookup WHERE normalized_address IN (",
      placeholders,
      ")"
    )
    dbGetQuery(con, sql, params = as.list(chunk))
  })

  dplyr::bind_rows(out)
}

# Attempts exact variant matching and then fuzzy fallback in local SQLite.
lookup_single_address_db <- function(con, address_value) {
  # Pass 1: exact match across canonicalized address variants.
  variants <- address_lookup_variants(address_value)
  variants <- variants[!is.na(variants) & nzchar(variants)]

  if (length(variants) > 0) {
    placeholders <- paste(rep("?", length(variants)), collapse = ",")
    sql_exact <- paste0(
      "SELECT normalized_address, geoid, address FROM address_geoid_lookup WHERE normalized_address IN (",
      placeholders,
      ") LIMIT 1"
    )
    exact_hit <- tryCatch(
      dbGetQuery(con, sql_exact, params = as.list(variants)),
      error = function(e) NULL
    )
    if (is.data.frame(exact_hit) && nrow(exact_hit) > 0) {
      return(exact_hit[1, , drop = FALSE])
    }
  }

  # Pass 2: targeted fuzzy match using house number + street core pattern.
  canonical <- canonicalize_address_text(address_value)
  if (!nzchar(canonical)) {
    return(NULL)
  }

  house_num <- sub("^([0-9]+[a-z]?).*$", "\\1", canonical)
  if (!grepl("^[0-9]", house_num)) {
    return(NULL)
  }

  street_core <- gsub(paste0("^", house_num, "\\s+"), "", canonical)
  street_core <- gsub("^(n|s|e|w|ne|nw|se|sw)\\s+", "", street_core)
  street_core <- trimws(street_core)

  if (!nzchar(street_core) || nchar(street_core) < 4) {
    return(NULL)
  }

  fuzzy_pattern <- paste0(house_num, " %", street_core, "%")
  sql_fuzzy <- paste0(
    "SELECT normalized_address, geoid, address FROM address_geoid_lookup ",
    "WHERE normalized_address LIKE ? LIMIT 1"
  )
  fuzzy_hit <- tryCatch(dbGetQuery(con, sql_fuzzy, params = list(fuzzy_pattern)), error = function(e) NULL)

  if (is.data.frame(fuzzy_hit) && nrow(fuzzy_hit) > 0) {
    return(fuzzy_hit[1, , drop = FALSE])
  }

  NULL
}

# Inserts/replaces lookup rows and refreshes table metadata counters.
upsert_lookup_rows <- function(con, rows_df) {
  # Drop malformed rows and require tract-length GEOID before writing.
  if (!is.data.frame(rows_df) || nrow(rows_df) == 0) {
    return(invisible(NULL))
  }

  rows_df <- rows_df %>%
    dplyr::filter(
      !is.na(.data$normalized_address),
      nzchar(.data$normalized_address),
      !is.na(.data$geoid),
      nzchar(.data$geoid)
    ) %>%
    dplyr::mutate(geoid = gsub("[^0-9]", "", .data$geoid)) %>%
    dplyr::filter(nchar(.data$geoid) >= 11)

  if (nrow(rows_df) == 0) {
    return(invisible(NULL))
  }

  sql <- paste0(
    "INSERT OR REPLACE INTO address_geoid_lookup(",
    "normalized_address, address, geoid, source, updated_at)",
    " VALUES (?, ?, ?, ?, datetime('now'))"
  )

  for (i in seq_len(nrow(rows_df))) {
    dbExecute(
      con,
      sql,
      params = list(
        rows_df$normalized_address[[i]],
        rows_df$address[[i]],
        rows_df$geoid[[i]],
        rows_df$source[[i]]
      )
    )
  }

  # Update metadata summary values after upsert for quick diagnostics.
  current_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM address_geoid_lookup")$n[[1]]
  dbExecute(
    con,
    "INSERT OR REPLACE INTO metadata(key, value) VALUES ('row_count', ?)",
    params = list(as.character(current_count))
  )
  dbExecute(
    con,
    "INSERT OR REPLACE INTO metadata(key, value) VALUES ('updated_at_utc', datetime('now'))"
  )
}

# Performs lightweight field normalization prior to join execution.
preclean_for_join <- function(df) {
  # Lightweight normalization used before full join pipeline kicks in.
  if (!is.data.frame(df) || nrow(df) == 0) {
    return(df)
  }

  local_df <- df

  address_col_name <- names(local_df)[tolower(names(local_df)) == "address"]
  if (length(address_col_name) == 1) {
    addr_col <- address_col_name[1]
    local_df[[addr_col]] <- toupper(trimws(gsub("\\s+", " ", as.character(local_df[[addr_col]]))))
    local_df$cleaned_address <- normalize_address_text(local_df[[addr_col]])
  }

  zip_col_name <- names(local_df)[tolower(names(local_df)) %in% c("zip", "zipcode", "zip_code")]
  if (length(zip_col_name) == 1) {
    zip_col <- zip_col_name[1]
    zip_digits <- substr(gsub("[^0-9]", "", as.character(local_df[[zip_col]])), 1, 5)
    local_df[[zip_col]] <- ifelse(nchar(zip_digits) == 5, zip_digits, NA_character_)
  }

  local_df
}

# Builds readable output names for selected Census variables.
build_readable_census_name_map <- function(selected_vars, vars_df) {
  # Build deterministic readable names from concept+label, with fallback to ID.
  vars_df <- ensure_variable_columns(vars_df)
  if (length(selected_vars) == 0) {
    return(setNames(character(0), character(0)))
  }

  selected_vars <- unique(as.character(selected_vars))
  mapped_names <- character(length(selected_vars))

  for (i in seq_along(selected_vars)) {
    var_id <- selected_vars[[i]]
    meta_row <- vars_df %>% dplyr::filter(.data$name == var_id) %>% dplyr::slice(1)

    raw_label <- if (nrow(meta_row) > 0 &&
      !is.na(meta_row$label[[1]]) &&
      nzchar(meta_row$label[[1]])) {
      meta_row$label[[1]]
    } else {
      var_id
    }

    raw_concept <- if (nrow(meta_row) > 0 &&
      !is.na(meta_row$concept[[1]]) &&
      nzchar(meta_row$concept[[1]])) {
      meta_row$concept[[1]]
    } else {
      ""
    }

    readable_base <- if (nzchar(raw_concept)) {
      paste(raw_concept, raw_label, sep = " ")
    } else {
      raw_label
    }

    cleaned_base <- sanitize_census_header(readable_base)
    if (!nzchar(cleaned_base)) {
      cleaned_base <- sanitize_census_header(var_id)
    }

    mapped_names[[i]] <- paste0("census_", cleaned_base)
  }

  mapped_names <- make.unique(mapped_names, sep = "_")
  setNames(mapped_names, paste0("census_", selected_vars))
}

# Applies readable Census header mappings to joined output columns.
apply_readable_census_headers <- function(df, name_map) {
  if (!is.data.frame(df) || length(name_map) == 0) {
    return(df)
  }

  old_names <- intersect(names(name_map), names(df))
  if (length(old_names) == 0) {
    return(df)
  }

  new_names <- unname(name_map[old_names])
  names(df)[match(old_names, names(df))] <- new_names
  df
}

# Enriches uploaded rows with GEOIDs from local lookup and optional
# incremental geocoding, then writes new matches back to cache.
attach_geoid_from_lookup <- function(df, db_path, geocode_limit = 300) {
  # Workflow:
  # 1) read existing lookup matches
  # 2) try local variant/fuzzy DB matching for misses
  # 3) geocode remaining misses (bounded)
  # 4) write new matches back into cache and return enriched data
  geoid_col_name <- names(df)[tolower(names(df)) == "geoid"]
  address_col_name <- names(df)[tolower(names(df)) == "address"]

  # Bail out early if address input field is unavailable.
  if (length(address_col_name) != 1) {
    return(list(
      data = df,
      matched = 0,
      used_lookup = FALSE,
      message = "No 'address' column found in upload; cannot use address-to-GEOID lookup."
    ))
  }

  # Initialize schema and connect to lookup cache.
  ensure_address_lookup_db(db_path)

  con <- tryCatch(dbConnect(RSQLite::SQLite(), db_path), error = function(e) NULL)
  if (is.null(con)) {
    return(list(
      data = df,
      matched = 0,
      used_lookup = FALSE,
      message = paste0("Could not open ", db_path, " for address-to-GEOID lookup.")
    ))
  }
  on.exit(dbDisconnect(con), add = TRUE)

  # Start with direct lookup table match by normalized address.
  local_df <- df
  local_df$lookup_normalized_address <- normalize_address_text(local_df[[address_col_name[1]]])

  lookup_tbl <- fetch_lookup_rows(con, local_df$lookup_normalized_address) %>%
    dplyr::rename(geoid_lookup = .data$geoid)

  local_df <- local_df %>%
    dplyr::left_join(lookup_tbl, by = c("lookup_normalized_address" = "normalized_address"))

  missing_idx <- which(is.na(local_df$geoid_lookup) | !nzchar(local_df$geoid_lookup))
  missing_unique <- unique(local_df$lookup_normalized_address[missing_idx])
  missing_unique <- missing_unique[!is.na(missing_unique) & nzchar(missing_unique)]

  db_matched_rows <- data.frame(
    normalized_address = character(0),
    address = character(0),
    geoid = character(0),
    source = character(0),
    stringsAsFactors = FALSE
  )

  # Attempt local DB variant/fuzzy fallback for still-missing addresses.
  if (length(missing_unique) > 0) {
    db_lookup_limit <- min(length(missing_unique), 1000)
    to_lookup <- missing_unique[seq_len(db_lookup_limit)]

    for (norm_addr in to_lookup) {
      representative_address <- local_df$address[match(norm_addr, local_df$lookup_normalized_address)]
      hit <- lookup_single_address_db(con, representative_address)
      if (is.data.frame(hit) && nrow(hit) > 0 && !is.na(hit$geoid[1]) && nzchar(hit$geoid[1])) {
        db_matched_rows <- dplyr::bind_rows(
          db_matched_rows,
          data.frame(
            normalized_address = norm_addr,
            address = as.character(representative_address),
            geoid = as.character(hit$geoid[1]),
            source = "lookup_db_match",
            stringsAsFactors = FALSE
          )
        )
      }
    }

    # Re-join refreshed lookup rows after successful fallback matches.
    if (nrow(db_matched_rows) > 0) {
      db_matched_rows <- db_matched_rows %>%
        dplyr::distinct(.data$normalized_address, .keep_all = TRUE)
      upsert_lookup_rows(con, db_matched_rows)

      refreshed_lookup <- fetch_lookup_rows(con, local_df$lookup_normalized_address) %>%
        dplyr::rename(geoid_lookup = .data$geoid)

      local_df <- local_df %>%
        dplyr::select(-dplyr::all_of("geoid_lookup")) %>%
        dplyr::left_join(refreshed_lookup, by = c("lookup_normalized_address" = "normalized_address"))
    }
  }

  missing_idx <- which(is.na(local_df$geoid_lookup) | !nzchar(local_df$geoid_lookup))
  missing_unique <- unique(local_df$lookup_normalized_address[missing_idx])
  missing_unique <- missing_unique[!is.na(missing_unique) & nzchar(missing_unique)]

  # Geocode a bounded set of still-missing addresses and upsert results.
  geocoded_rows <- data.frame(
    normalized_address = character(0),
    address = character(0),
    geoid = character(0),
    source = character(0),
    stringsAsFactors = FALSE
  )

  geocoded_count <- 0
  geocode_attempts <- min(length(missing_unique), geocode_limit)

  if (geocode_attempts > 0) {
    to_geocode <- missing_unique[seq_len(geocode_attempts)]

    for (norm_addr in to_geocode) {
      representative_address <- local_df$address[match(norm_addr, local_df$lookup_normalized_address)]
      geoid_val <- geocode_address_to_geoid(representative_address)

      if (!is.na(geoid_val) && nzchar(geoid_val)) {
        geocoded_rows <- dplyr::bind_rows(
          geocoded_rows,
          data.frame(
            normalized_address = norm_addr,
            address = as.character(representative_address),
            geoid = geoid_val,
            source = "census_geocoder",
            stringsAsFactors = FALSE
          )
        )
      }
    }

    geocoded_count <- nrow(geocoded_rows)
    upsert_lookup_rows(con, geocoded_rows)

    # Re-join lookup rows again to include newly geocoded entries.
    if (geocoded_count > 0) {
      fresh_lookup <- fetch_lookup_rows(con, local_df$lookup_normalized_address) %>%
        dplyr::rename(geoid_lookup = .data$geoid)

      local_df <- local_df %>%
        dplyr::select(-dplyr::all_of("geoid_lookup")) %>%
        dplyr::left_join(fresh_lookup, by = c("lookup_normalized_address" = "normalized_address"))
    }
  }

  # Merge resolved lookup GEOIDs into existing GEOID column when present.
  lookup_matches <- sum(!is.na(local_df$geoid_lookup) & nzchar(local_df$geoid_lookup))

  if (length(geoid_col_name) == 1) {
    existing_vals <- as.character(local_df[[geoid_col_name[1]]])
    missing_existing <- is.na(existing_vals) | !nzchar(trimws(existing_vals))
    local_df[[geoid_col_name[1]]][missing_existing] <-
      local_df$geoid_lookup[missing_existing]
  } else {
    local_df$GEOID <- local_df$geoid_lookup
  }

  local_df <- local_df %>%
    dplyr::select(-dplyr::all_of(c("lookup_normalized_address", "geoid_lookup")))

  db_match_count <- nrow(db_matched_rows)

  # Return enriched data + summary message used in notifications.
  list(
    data = local_df,
    matched = lookup_matches,
    used_lookup = TRUE,
    message = paste0(
      "Address lookup matched ", lookup_matches,
      " row(s) from local DB (", db_match_count, " normalized by variant matching), ",
      " and ", geocoded_count, " newly geocoded this run)."
    )
  )
}

# --- 1.4 Catalog Helpers ---
# The catalog is a fully hardcoded, verified list of every year/dataset
# combination confirmed to work with tidycensus's get_acs() and
# get_decennial() functions as of 2026. This replaces live API discovery,
# which was unreliable (returned entries with no usable variable metadata).

# Returns a data frame of all verified Census year/dataset combinations.
static_census_catalog <- function() {
  # ACS 5-Year Detailed Tables
  # Geography: state, county, tract, block group, zcta
  # Note: year Y released ~December Y+1. 2024 released Dec 2025.
  acs5_years <- 2009:2024

  # ACS 1-Year Detailed Tables
  # Geography: state, county
  # Note: 2020 was NOT published due to COVID-19 data collection issues.
  acs1_years <- c(2005:2019, 2021:2024)

  # ACS 3-Year Detailed Tables (retired after 2013)
  # Geography: state, county
  acs3_years <- 2007:2013

  # ACS 1-Year Supplemental Estimates (discontinued after 2022)
  # Geography: state, county
  acsse_years <- 2014:2022

  # Decennial Redistricting Data (P.L. 94-171)
  # Geography: state, county, tract, block group
  pl_years <- c(2000L, 2010L, 2020L)

  # Decennial Demographic & Housing Characteristics (2020 Census replacement for SF1)
  # Geography: state, county, tract, block group
  dhc_years <- 2020L

  # Decennial Summary File 1 (replaced by DHC for 2020+)
  # Geography: state, county, tract, block group
  sf1_years <- c(2000L, 2010L)

  # Decennial Summary File 3 (2000 only; SF3 was not produced for 2010)
  # Geography: state, county, tract
  sf3_years <- 2000L

  rbind(
    data.frame(
      name               = "acs5",
      vintage            = acs5_years,
      display_name       = paste0("ACS 5-Year Detailed Tables (", acs5_years, ")"),
      tidycensus_dataset = "acs5",
      stringsAsFactors   = FALSE
    ),
    data.frame(
      name               = "acs1",
      vintage            = acs1_years,
      display_name       = paste0("ACS 1-Year Detailed Tables (", acs1_years, ")"),
      tidycensus_dataset = "acs1",
      stringsAsFactors   = FALSE
    ),
    data.frame(
      name               = "acs3",
      vintage            = acs3_years,
      display_name       = paste0("ACS 3-Year Detailed Tables (", acs3_years, ") [retired]"),
      tidycensus_dataset = "acs3",
      stringsAsFactors   = FALSE
    ),
    data.frame(
      name               = "acsse",
      vintage            = acsse_years,
      display_name       = paste0("ACS 1-Year Supplemental Estimates (", acsse_years, ") [retired]"),
      tidycensus_dataset = "acsse",
      stringsAsFactors   = FALSE
    ),
    data.frame(
      name               = "pl",
      vintage            = pl_years,
      display_name       = paste0("Decennial Redistricting Data - PL 94-171 (", pl_years, ")"),
      tidycensus_dataset = "pl",
      stringsAsFactors   = FALSE
    ),
    data.frame(
      name               = "dhc",
      vintage            = dhc_years,
      display_name       = paste0("Decennial Demographic & Housing Characteristics (", dhc_years, ")"),
      tidycensus_dataset = "dhc",
      stringsAsFactors   = FALSE
    ),
    data.frame(
      name               = "sf1",
      vintage            = sf1_years,
      display_name       = paste0("Decennial Summary File 1 (", sf1_years, ")"),
      tidycensus_dataset = "sf1",
      stringsAsFactors   = FALSE
    ),
    data.frame(
      name               = "sf3",
      vintage            = sf3_years,
      display_name       = paste0("Decennial Summary File 3 (", sf3_years, ")"),
      tidycensus_dataset = "sf3",
      stringsAsFactors   = FALSE
    )
  )
}

# Returns the verified static catalog. Replaces the old single-year fallback.
default_catalog_data <- function() {
  static_census_catalog()
}

# Returns the verified static catalog. Replaces the old live censusapi scan,
# which returned catalog entries that often had no loadable variable metadata.
fetch_catalog_data <- function() {
  static_census_catalog()
}
