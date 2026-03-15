# -----------------------------------------------------------------------------
# File: scripts/R/server.R
# Purpose: Implement all reactive/server-side behavior for Censusight.
# Responsibilities:
# - Manage session state, authentication, and upload processing
# - Load/refresh Census catalogs and variable metadata
# - Execute address cleaning, GEOID enrichment, and Census joins
# - Produce user feedback, previews, downloads, and status messaging
# -----------------------------------------------------------------------------

# --- 3. Server Logic ---

server <- function(input, output, session) {
  # --- 3.1 Runtime Paths & Guardrails ---
  # Local lookup cache path and guardrail for live geocoding per run.
  address_lookup_db_path <- censusight_data_path("address_geoid.sqlite", must_exist = TRUE)
  geocode_limit_per_run <- 300

  # --- 3.2 Startup URL Logging ---
  # Emits the active app URL once the client handshake has enough metadata.
  session$onFlushed(function() {
    isolate({
      protocol <- session$clientData$url_protocol
      hostname <- session$clientData$url_hostname
      port <- session$clientData$url_port

      if (is.null(protocol) || !nzchar(protocol)) {
        protocol <- "http:"
      }
      if (is.null(hostname) || !nzchar(hostname)) {
        hostname <- "127.0.0.1"
      }

      app_url <- paste0(protocol, "//", hostname)
      if (!is.null(port) && nzchar(port)) {
        app_url <- paste0(app_url, ":", port)
      }

      cat(sprintf("\nCensusight is running at: %s\n\n", app_url))
    })
  }, once = TRUE)

  # --- 3.3 Shared Progress/Quota Utilities ---
  # Wrapper keeps progress-bar updates consistent across all server handlers.
  update_progress_bar <- function(id, value, status, title = NULL, total = NULL) {
    args <- list(
      session = session,
      id = id,
      value = value,
      status = status
    )

    if (!is.null(title)) {
      args$title <- title
    }

    if (!is.null(total)) {
      args$total <- total
    }

    do.call(shinyWidgets::updateProgressBar, args)
  }

  # Reactive counters for API request tracking and remaining quota estimates.
  api_request_count <- reactiveVal(0L)
  api_quota_remaining <- reactiveVal(500)
  api_quota_mode <- reactiveVal("session")
 # Session-based quota tracking assumes a 500-request limit and counts requests made during this session.
  get_session_quota_remaining <- function() {
    current <- suppressWarnings(as.integer(isolate(api_request_count())))
    if (is.na(current)) {
      current <- 0L
    }
    max(500 - as.numeric(current), 0)
  }
  # increments the API request counter by a specified number (default 1) to track usage during the session.
  increment_api_counter <- function(n = 1L) {
    current <- suppressWarnings(as.integer(isolate(api_request_count())))
    if (is.na(current)) {
      current <- 0L
    }
    api_request_count(current + as.integer(n))
  }

  # Estimate available quota using API headers when possible, with session fallback.
  check_census_quota <- function() {
    api_key <- Sys.getenv("CENSUS_API_KEY")

    if (!nzchar(api_key)) {
      return(list(mode = "session", remaining = get_session_quota_remaining()))
    }

    test_url <- paste0(
      "https://api.census.gov/data/2020/dec/pl?get=NAME&for=state:55&key=",
      api_key
    )

    response <- tryCatch(
      GET(test_url, timeout(5)),
      error = function(e) NULL
    )
    increment_api_counter(1L)

    if (is.null(response)) {
      return(list(mode = "session", remaining = get_session_quota_remaining()))
    }

    quota <- headers(response)[["x-ratelimit-remaining"]]
    quota_num <- suppressWarnings(as.numeric(quota))

    if (!is.na(quota_num)) {
      return(list(mode = "header", remaining = quota_num))
    }

    list(mode = "session", remaining = get_session_quota_remaining())
  }
  # Check quota at startup and update reactive values accordingly.
  observe({
    try({
      quota_info <- check_census_quota()
      api_quota_mode(quota_info$mode)
      api_quota_remaining(quota_info$remaining)
    }, silent = TRUE)
  })

  # --- 3.4 Census Catalog & Variable Cache Bootstrap ---
  supported_datasets <- c(
    "acs1", "acs3", "acs5", "acsse",
    "pl", "dhc", "sf1", "sf3"
  )
  # catalog_path and variable_catalog_path define where the app will persist the Census dataset catalog and variable metadata cache
  # respectively, to optimize loading on subsequent runs.
  catalog_path <- censusight_data_path("census_catalog.rds", must_exist = TRUE)
  variable_catalog_path <- censusight_data_path("census_variable_catalog.rds", must_exist = TRUE)
  # available_datasets holds the currently loaded Census dataset catalog
  # variable_cache stores metadata for variables by dataset/year
  # invalid_dataset_keys tracks datasets that failed to load variable metadata to avoid repeated attempts.
  available_datasets <- reactiveVal(NULL)
  variable_cache <- reactiveVal(list())
  invalid_dataset_keys <- reactiveVal(character(0))

  # Load persisted variable metadata cache if available.
  if (file.exists(variable_catalog_path)) {
    cached_variables <- tryCatch(readRDS(variable_catalog_path), error = function(e) list())
    if (is.list(cached_variables)) {
      variable_cache(cached_variables)
    }
  }

  # Always initialize the dataset catalog from the verified static catalog.
  # The old live-API discovery (censusapi::listCensusApis) is no longer used
  # because it returned entries with no loadable variable metadata, causing
  # silent failures in the join pipeline. Every entry in the static catalog
  # has been confirmed to work with tidycensus get_acs() / get_decennial().
  startup_catalog <- default_catalog_data()
  saveRDS(startup_catalog, catalog_path)
  available_datasets(startup_catalog)

  # --- 3.5 Manual Catalog Refresh Actions ---
  observeEvent(input$update_catalog_btn, {
    # Reload from the hardcoded static catalog. All entries are pre-verified
    # to work with tidycensus, so no live API scan is needed.
    catalog_data <- default_catalog_data()
    saveRDS(catalog_data, catalog_path)
    available_datasets(catalog_data)

    showNotification(
      paste0(
        "Catalog reloaded. ",
        nrow(catalog_data),
        " verified year/dataset combinations available."
      ),
      type = "message"
    )
  })

  # Refresh gauge values after catalog update attempts.
  observeEvent(input$update_catalog_btn, {
    try({
      quota_info <- check_census_quota()
      api_quota_mode(quota_info$mode)
      api_quota_remaining(quota_info$remaining)
    }, silent = TRUE)
  })

  # --- 3.6 Catalog-Derived Reactive Selectors ---
  # Restrict catalog to datasets/vintages this app can actually query.
  catalog_data_filtered <- reactive({
    data <- available_datasets()

    if (!is.data.frame(data) || !all(c("name", "vintage", "display_name") %in% names(data))) {
      data <- default_catalog_data()
    }

    if (!"tidycensus_dataset" %in% names(data)) {
      data$tidycensus_dataset <- NA_character_
    }
    # Normalize dataset names, coalescing from both 'tidycensus_dataset' and 'name' fields, then filter to supported datasets only.
    normalized <- data %>%
      dplyr::mutate(
        name = as.character(.data$name),
        vintage = parse_vintage_year(.data$vintage),
        tidycensus_dataset = dplyr::coalesce(
          normalize_dataset_name(.data$tidycensus_dataset),
          normalize_dataset_name(.data$name)
        ),
        display_name = dplyr::coalesce(
          as.character(.data$display_name),
          as.character(.data$name)
        )
      ) %>%
      dplyr::filter(
        !is.na(.data$vintage),
        !is.na(.data$tidycensus_dataset),
        .data$tidycensus_dataset %in% supported_datasets
      )

    if (nrow(normalized) == 0) {
      normalized <- default_catalog_data()
    }

    normalized
  })

  # Recompute available years whenever filtered catalog changes.
  observeEvent(catalog_data_filtered(), {
    data <- catalog_data_filtered()
    req(data)
    # Extract unique vintage years from the catalog data, sort them in decreasing order, and update the year selection input. 
    # If there are no valid years, clear the selection input.
    years <- sort(unique(parse_vintage_year(data$vintage)), decreasing = TRUE)
    years <- years[!is.na(years)]
    if (length(years) == 0) {
      freezeReactiveValue(input, "census_year")
      updateSelectInput(
        session,
        "census_year",
        choices = character(0),
        selected = character(0)
      )
      return()
    }

    selected_year <- isolate(suppressWarnings(as.integer(input$census_year)))
    if (length(selected_year) != 1 || is.na(selected_year) || !(selected_year %in% years)) {
      selected_year <- years[1]
    }

    freezeReactiveValue(input, "census_year")
    updateSelectInput(session, "census_year", choices = years, selected = selected_year)
  }, ignoreInit = FALSE)

  # Rebuild dataset choices whenever year or catalog changes.
  observeEvent(list(catalog_data_filtered(), input$census_year), {
    data <- catalog_data_filtered()

    if (is.null(data)) {
      freezeReactiveValue(input, "census_dataset")
      updateSelectInput(
        session,
        "census_dataset",
        choices = character(0),
        selected = character(0)
      )
      return()
    }
    # Extract the selected year, filter the catalog to datasets matching that vintage, and update the dataset selection input with display names.
    selected_year <- suppressWarnings(as.integer(input$census_year))
    if (length(selected_year) != 1 || is.na(selected_year)) {
      freezeReactiveValue(input, "census_dataset")
      updateSelectInput(
        session,
        "census_dataset",
        choices = character(0),
        selected = character(0)
      )
      return()
    }
    # For datasets matching the selected vintage, construct display names that include geography levels, then update the dataset selection input.
    data_for_year <- data %>%
      dplyr::mutate(vintage = parse_vintage_year(.data$vintage)) %>%
      dplyr::filter(.data$vintage == selected_year) %>%
      dplyr::arrange(.data$display_name) %>%
      dplyr::mutate(
        display_with_levels = paste0(
          .data$display_name,
          " [",
          dataset_geography_levels(.data$tidycensus_dataset),
          "]"
        )
      )
    # Before updating the dataset choices, attempt to load variable metadata for each dataset to ensure they are queryable. 
    # If metadata loading fails for a dataset, exclude it from the choices and track it in invalid_dataset_keys to avoid repeated load attempts.
    if (nrow(data_for_year) > 0) {
      existing_cache <- isolate(variable_cache())
      bad_keys <- isolate(invalid_dataset_keys())
      still_bad <- character(0)
      keep_rows <- rep(FALSE, nrow(data_for_year))

      # Only open a progress bar when at least one dataset actually needs live-fetching.
      any_needs_fetch <- any(vapply(seq_len(nrow(data_for_year)), function(i) {
        ds_norm <- normalize_dataset_name(data_for_year$tidycensus_dataset[i])
        if (is.na(ds_norm)) return(FALSE)
        key <- paste(selected_year, ds_norm, sep = "|")
        cached <- existing_cache[[key]]
        !(is.data.frame(cached) && nrow(cached) > 0)
      }, logical(1)))

      n_ds <- nrow(data_for_year)
      catalog_prog <- if (any_needs_fetch) {
        p <- shiny::Progress$new(session, min = 0, max = n_ds)
        p$set(
          message = paste0("Loading variable catalog for ", selected_year, "..."),
          value = 0
        )
        p
      } else {
        NULL
      }

      for (i in seq_len(n_ds)) {
        if (!is.null(catalog_prog)) {
          catalog_prog$set(value = i, detail = paste0("Dataset ", i, " of ", n_ds))
        }

        ds_norm <- normalize_dataset_name(data_for_year$tidycensus_dataset[i])
        key <- paste(selected_year, ds_norm, sep = "|")

        if (is.na(ds_norm)) {
          still_bad <- c(still_bad, key)
          next
        }

        cached <- existing_cache[[key]]
        if (is.data.frame(cached) && nrow(cached) > 0) {
          keep_rows[i] <- TRUE
          next
        }

        vars_try <- tryCatch(
          ensure_variable_columns(load_variables(selected_year, ds_norm, cache = TRUE)),
          error = function(e) NULL
        )

        if (is.data.frame(vars_try) && nrow(vars_try) > 0) {
          existing_cache[[key]] <- vars_try
          keep_rows[i] <- TRUE
        } else {
          still_bad <- c(still_bad, key)
        }
      }

      if (!is.null(catalog_prog)) catalog_prog$close()

      # Update the variable cache and invalid dataset keys based on the results of metadata loading attempts
      # Then filter the catalog data to only include datasets with valid metadata before updating the dataset selection input.
      variable_cache(existing_cache)
      invalid_dataset_keys(unique(c(
        setdiff(bad_keys, paste(selected_year, data_for_year$tidycensus_dataset, sep = "|")),
        still_bad
      )))
      data_for_year <- data_for_year[keep_rows, , drop = FALSE]
    }
    # If the currently selected dataset is not in the filtered list for the selected year, attempt to fallback to a different dataset from the same year. 
    #If no datasets are available for that year, clear the selection input.
    if (nrow(data_for_year) == 0) {
      fallback_year <- data %>%
        dplyr::mutate(vintage = parse_vintage_year(.data$vintage)) %>%
        dplyr::filter(!is.na(.data$vintage)) %>%
        dplyr::summarise(y = max(.data$vintage, na.rm = TRUE)) %>%
        dplyr::pull("y")

      if (length(fallback_year) == 1 && !is.na(fallback_year) && fallback_year != selected_year) {
        freezeReactiveValue(input, "census_year")
        updateSelectInput(session, "census_year", selected = fallback_year)
      }
    }

    choices <- setNames(data_for_year$name, data_for_year$display_with_levels)

    current_dataset <- isolate(input$census_dataset)
    selected_dataset <- if (!is.null(current_dataset) &&
      !is.na(current_dataset) &&
      current_dataset %in% data_for_year$name) {
      current_dataset
    } else if (nrow(data_for_year) > 0) {
      data_for_year$name[1]
    } else {
      character(0)
    }

    freezeReactiveValue(input, "census_dataset")
    updateSelectInput(
      session,
      "census_dataset",
      choices = choices,
      selected = selected_dataset
    )
  }, ignoreInit = FALSE)

  # --- 3.7 Variable Metadata Loading & Filtering ---
  # Resolves variables for selected year/dataset with cache-first behavior.
  var_list <- reactive({
    req(input$census_dataset, input$census_year)

    year_raw <- suppressWarnings(as.integer(input$census_year))
    ds_raw <- input$census_dataset
    ds <- normalize_dataset_name(ds_raw)
    cache_key <- paste(year_raw, ds, sep = "|")

    if (is.na(ds)) {
      bad_keys <- isolate(invalid_dataset_keys())
      invalid_dataset_keys(unique(c(bad_keys, cache_key)))

      showNotification(
        paste0("Dataset '", ds_raw, "' is not supported by tidycensus variable lookup."),
        type = "warning",
        duration = 7
      )

      return(data.frame(
        name = character(0),
        label = character(0),
        concept = character(0),
        stringsAsFactors = FALSE
      ))
    }

    existing_cache <- variable_cache()
    if (!is.null(existing_cache[[cache_key]]) &&
      is.data.frame(existing_cache[[cache_key]]) &&
      nrow(existing_cache[[cache_key]]) > 0) {
      bad_keys <- isolate(invalid_dataset_keys())
      if (cache_key %in% bad_keys) {
        invalid_dataset_keys(setdiff(bad_keys, cache_key))
      }
      return(fill_dataset_geography(ensure_variable_columns(existing_cache[[cache_key]]), ds))
    }

    var_prog <- shiny::Progress$new(session, min = 0, max = 1)
    on.exit(var_prog$close(), add = TRUE)
    var_prog$set(
      message = paste0("Loading ", ds, " variables (", year_raw, ")..."),
      value = 0.1,
      detail = "Contacting Census API..."
    )
    tryCatch(
      {
        vars <- fill_dataset_geography(
          ensure_variable_columns(load_variables(year_raw, ds, cache = TRUE)),
          ds
        )
        var_prog$set(value = 0.9, detail = "Caching metadata...")
        if (is.data.frame(vars) && nrow(vars) > 0) {
          existing_cache[[cache_key]] <- vars
          variable_cache(existing_cache)
          saveRDS(existing_cache, variable_catalog_path)

          bad_keys <- isolate(invalid_dataset_keys())
          if (cache_key %in% bad_keys) {
            invalid_dataset_keys(setdiff(bad_keys, cache_key))
          }
        } else {
          bad_keys <- isolate(invalid_dataset_keys())
          invalid_dataset_keys(unique(c(bad_keys, cache_key)))
        }
        var_prog$set(value = 1.0)
        vars
      },
      error = function(e) {
        bad_keys <- isolate(invalid_dataset_keys())
        invalid_dataset_keys(unique(c(bad_keys, cache_key)))

        showNotification(
          paste0(
            "No variable metadata is available for '",
            ds_raw,
            "' in ",
            year_raw,
            "."
          ),
          type = "warning",
          duration = 7
        )

        data.frame(
          name = character(0),
          label = character(0),
          concept = character(0),
          geography = character(0),
          stringsAsFactors = FALSE
        )
      }
    )
  })

  # Updates selectable variable list based on dataset, upload columns,
  # inferred join geography, and state-specific compatibility rules.
  observe({
    req(var_list())
    vars <- var_list()

    # Guard against empty/invalid metadata payloads before building choices.
    if (nrow(vars) == 0 || !all(c("name", "label", "concept") %in% names(vars))) {
      updateSelectizeInput(
        session,
        "census_vars",
        choices = character(0),
        selected = character(0),
        server = TRUE
      )
      return()
    }

    vars <- ensure_variable_columns(vars)

    # Determine geography capabilities implied by selected dataset type.
    # county_capable is restricted to surveys with complete county coverage;
    # ACS 1-Year/3-Year/ACSSE cover only large counties so restrict to state.
    ds <- normalize_dataset_name(input$census_dataset)
    county_capable <- !is.na(ds) && ds %in% c("acs5", "pl", "dhc", "sf1", "sf3")
    tract_capable <- !is.na(ds) && ds %in% c("acs5", "pl", "dhc", "sf1", "sf3")
    zcta_capable <- !is.na(ds) && ds %in% c("acs5")

    # Prefer processed data (post-cleaning) when available; otherwise raw upload.
    df <- uploaded_data()
    if (exists("APP_STATE", inherits = FALSE) &&
      !is.null(APP_STATE$processed_data) &&
      is.data.frame(APP_STATE$processed_data)) {
      df <- APP_STATE$processed_data
    }

    # Inspect upload columns to infer whether GEOID and/or ZIP joins are possible.
    has_geoid <- FALSE
    has_zip <- FALSE
    if (!is.null(df) && is.data.frame(df)) {
      geoid_col <- names(df)[tolower(names(df)) == "geoid"]
      has_geoid <- length(geoid_col) >= 1

      zip_col <- names(df)[tolower(names(df)) %in% c("zip", "zipcode", "zip_code")]
      if (length(zip_col) >= 1) {
        zip_values <- unique(
          substr(gsub("[^0-9]", "", as.character(df[[zip_col[1]]])), 1, 5)
        )
        zip_values <- zip_values[nchar(zip_values) == 5]
        has_zip <- length(zip_values) > 0
      }
    }

    # Compute best geography in auto mode, then apply explicit user override
    # only when required columns and dataset capabilities are present.
    auto_geography <- if (has_geoid && tract_capable) {
      "tract"
    } else if (has_geoid && county_capable) {
      "county"
    } else if (has_zip && zcta_capable) {
      "zcta"
    } else {
      "state"
    }

    requested_geography <- as.character(input$join_geography)
    if (length(requested_geography) != 1 ||
      !requested_geography %in% c("auto", "tract", "county", "zcta", "state")) {
      requested_geography <- "auto"
    }

    effective_geography <- auto_geography
    if (requested_geography == "tract" && has_geoid && tract_capable) {
      effective_geography <- "tract"
    } else if (requested_geography == "county" && has_geoid && county_capable) {
      effective_geography <- "county"
    } else if (requested_geography == "zcta" && has_zip && zcta_capable) {
      effective_geography <- "zcta"
    } else if (requested_geography == "state") {
      effective_geography <- "state"
    }

    # Keep only variables available at the effective geography.
    availability <- vapply(
      vars$geography,
      variable_available_for_geography,
      FUN.VALUE = logical(1),
      target_geography = effective_geography
    )
    available_vars <- vars[availability, , drop = FALSE]

    # Apply Wisconsin-specific filtering (e.g., excluding PR-only variables).
    if (nrow(available_vars) > 0) {
      state_availability <- mapply(
        variable_allowed_for_state,
        available_vars$label,
        available_vars$concept,
        MoreArgs = list(state_abbr = "WI")
      )
      available_vars <- available_vars[state_availability, , drop = FALSE]
    }

    if (nrow(available_vars) == 0) {
      updateSelectizeInput(
        session,
        "census_vars",
        choices = character(0),
        selected = character(0),
        server = TRUE
      )
      return()
    }

    # Build user-facing labels that include concept/geography context.
    available_levels <- vapply(
      available_vars$geography,
      format_variable_geography,
      FUN.VALUE = character(1)
    )
    choice_vec <- available_vars$name
    names(choice_vec) <- paste0(
      available_vars$label,
      " (",
      available_vars$concept,
      ") — ",
      available_levels
    )

    # Preserve still-valid selections and notify when some were auto-removed.
    current_selected <- isolate(unique(as.character(input$census_vars)))
    current_selected <- current_selected[nzchar(current_selected)]
    valid_selected <- intersect(current_selected, available_vars$name)

    if (length(current_selected) > length(valid_selected)) {
      showNotification(
        "Some selected variables were removed because they are not available for the active join geography.",
        type = "warning",
        duration = 6
      )
    }

    updateSelectizeInput(
      session,
      "census_vars",
      choices = choice_vec,
      selected = valid_selected,
      server = TRUE
    )
  })

  # --- 3.8 UI Status Outputs (Dataset Count + API Gauge) ---
  output$census_dataset_count <- renderText({
    req(available_datasets(), input$census_year)

    data <- available_datasets()
    if (!is.data.frame(data) || !all(c("name", "vintage") %in% names(data))) {
      return(NULL)
    }

    selected_year <- suppressWarnings(as.integer(input$census_year))
    if (is.na(selected_year)) {
      return(NULL)
    }

    if (!"tidycensus_dataset" %in% names(data)) {
      data$tidycensus_dataset <- NA_character_
    }

    # Count only datasets that normalize to supported tidycensus identifiers.
    count_for_year <- data %>%
      dplyr::mutate(
        vintage = parse_vintage_year(.data$vintage),
        tidycensus_dataset = dplyr::coalesce(
          normalize_dataset_name(.data$tidycensus_dataset),
          normalize_dataset_name(.data$name)
        )
      ) %>%
      dplyr::filter(
        .data$vintage == selected_year,
        !is.na(.data$tidycensus_dataset)
      ) %>%
      dplyr::summarise(n = n()) %>%
      dplyr::pull(n)

    paste0(count_for_year, " datasets available for ", selected_year)
  })

  # Drives the sidebar quota gauge from either header-based or session-based
  # request accounting.
  observe({
    mode <- api_quota_mode()
    remaining <- suppressWarnings(as.numeric(api_quota_remaining()))
    req_count <- suppressWarnings(as.integer(api_request_count()))

    if (is.na(req_count) || req_count < 0) {
      req_count <- 0L
    }

    if (is.na(remaining)) {
      remaining <- max(500 - req_count, 0)
    }

    total <- 500
    title <- if (identical(mode, "header")) {
      "Pulls Remaining (header)"
    } else {
      paste0("Session Census Calls: ", req_count)
    }

    # Translate quota values into semantic gauge states.
    bar_status <- "success"
    if (remaining < 100) {
      bar_status <- "warning"
    }
    if (remaining < 50) {
      bar_status <- "danger"
    }

    update_progress_bar(
      id = "api_gauge",
      value = remaining,
      status = bar_status,
      title = title,
      total = total
    )
  })

  # --- 3.9 Session State Containers ---
  # USER tracks auth role; APP_STATE tracks generated data products.
  USER <- reactiveValues(logged_in = FALSE, role = NULL)
  APP_STATE <- reactiveValues(
    error_data = NULL,
    joined_data = NULL,
    preview_selection = NULL,
    processed_data = NULL
  )

  # Centralized helper to clear all derived app outputs.
  reset_app_state <- function() {
    APP_STATE$error_data <- NULL
    APP_STATE$joined_data <- NULL
    APP_STATE$preview_selection <- NULL
    APP_STATE$processed_data <- NULL
  }

  # After login, initialize year/dataset selectors to latest valid defaults.
  observeEvent(USER$logged_in, {
    req(isTRUE(USER$logged_in))

    # Initialize selector defaults only after a successful login transition.
    data <- catalog_data_filtered()
    if (!is.data.frame(data) || nrow(data) == 0) {
      return()
    }

    years <- sort(unique(parse_vintage_year(data$vintage)), decreasing = TRUE)
    years <- years[!is.na(years)]
    if (length(years) == 0) {
      return()
    }

    selected_year <- years[1]

    data_for_year <- data %>%
      dplyr::mutate(vintage = parse_vintage_year(.data$vintage)) %>%
      dplyr::filter(.data$vintage == selected_year) %>%
      dplyr::arrange(.data$display_name) %>%
      dplyr::mutate(
        display_with_levels = paste0(
          .data$display_name,
          " [",
          dataset_geography_levels(.data$tidycensus_dataset),
          "]"
        )
      )

    if (nrow(data_for_year) == 0) {
      return()
    }

    choices <- setNames(data_for_year$name, data_for_year$display_with_levels)
    selected_dataset <- data_for_year$name[1]

    # Push default year/dataset once UI is flushed to avoid race conditions.
    session$onFlushed(function() {
      updateSelectInput(session, "census_year", choices = years, selected = selected_year)
      updateSelectInput(
        session,
        "census_dataset",
        choices = choices,
        selected = selected_dataset
      )
    }, once = TRUE)
  }, ignoreInit = TRUE)

  # --- 3.10 Dynamic Page Rendering ---
  # Renders auth card pre-login and full workflow layout after login.
  output$page_content <- renderUI({
    # Login view is a gated entry screen.
    if (USER$logged_in == FALSE) {
      div(
        style = "max-width: 400px; margin: 100px auto;",
        card(
          card_header("Please Log In", class = "bg-primary text-white"),
          card_body(
            textInput("username", "Username"),
            passwordInput("password", "Password"),
            actionButton("login_btn", "Log In", class = "btn-primary w-100"),
            hr(),
            p("Testing the app?", class = "text-center text-muted"),
            actionButton("guest_btn", "Continue as Guest", class = "btn-outline-info w-100")
          )
        )
      )
    } else {
      # Main app layout includes upload controls, preview, and join actions.
      layout_sidebar(
        sidebar = sidebar(
          h4(paste("Welcome,", USER$role, "!")),
          actionButton("logout_btn", "Log Out", class = "btn-danger"),
          hr(),
          fileInput(
            "csv_upload",
            "Upload CSV File",
            accept = c("text/csv", "text/comma-separated-values,text/plain", ".csv")
          ),
          checkboxInput("auto_clean", "Enable Automatic Address Cleaning", value = FALSE),
          actionButton("process_btn", "Process & Save to SQLite", class = "btn-success"),
          hr(),
          h6("Census API Gas Gauge"),
          shinyWidgets::progressBar(
            id = "api_gauge",
            value = 500,
            total = 500,
            title = "Pulls Remaining",
            status = "success"
          )
        ),
        div(
          card(
            card_header("Data Preview"),
            card_body(
              div(class = "text-muted mb-2", textOutput("preview_summary")),
              tableOutput("data_preview"),
              textOutput("status_message")
            )
          ),
          card(
            card_header("Census Variable Selection", class = "bg-dark text-white"),
            card_body(
              layout_column_wrap(
                width = 1 / 2,
                selectInput(
                  "census_year",
                  "Select Year",
                  choices = character(0),
                  selected = character(0),
                  selectize = FALSE
                ),
                selectInput(
                  "census_dataset",
                  "Select Dataset",
                  choices = character(0),
                  selected = character(0),
                  selectize = FALSE
                ),
                selectInput(
                  "join_geography",
                  "Join Geography",
                  choices = c(
                    "Auto (recommended)" = "auto",
                    "Tract" = "tract",
                    "ZIP Code (ZCTA)" = "zcta",
                    "State" = "state"
                  ),
                  selected = "auto",
                  selectize = FALSE
                ),
                actionButton(
                  "update_catalog_btn",
                  "Update Available Census Data",
                  icon = icon("sync"),
                  class = "btn-info btn-sm"
                ),
                actionButton(
                  "preview_btn",
                  "Preview",
                  class = "btn-outline-primary btn-sm"
                )
              ),
              div(class = "text-muted small mt-2", textOutput("census_dataset_count")),
              selectizeInput(
                "census_vars",
                "Select & Search Census Variables",
                choices = NULL,
                multiple = TRUE,
                options = list(placeholder = "Type to search...")
              )
            )
          ),
          card(
            card_body(
              actionButton(
                "run_join",
                "Perform Spatial Join with Census Data",
                class = "btn-primary w-100",
                icon = icon("database")
              )
            )
          ),
          uiOutput("error_report_card")
        )
      )
    }
  })

  # Conditionally renders downloadable error report card for invalid rows.
  output$error_report_card <- renderUI({
    # Hide card when there are no rejected rows to report.
    if (is.null(APP_STATE$error_data) || nrow(APP_STATE$error_data) == 0) {
      return(NULL)
    }

    card(
      card_header("🚨 Error Report: Invalid Addresses", class = "bg-danger text-white"),
      card_body(
        p("The following rows did not match our Wisconsin parcel database:"),
        tableOutput("error_table"),
        downloadButton("download_errors", "Download Error Report", class = "btn-warning mt-2")
      )
    )
  })

  # --- 3.11 Authentication & Session Reset Events ---
  observeEvent(input$login_btn, {
    # Minimal demo auth gate for local/admin testing workflows.
    if (input$username == "admin" && input$password == "password123") {
      USER$logged_in <- TRUE
      USER$role <- "Admin"
    } else {
      showNotification("Invalid username or password!", type = "error")
    }
  })

  # Guest mode bypass for exploratory testing.
  observeEvent(input$guest_btn, {
    USER$logged_in <- TRUE
    USER$role <- "Guest Tester"
  })

  # Full logout clears auth and all computed app state.
  observeEvent(input$logout_btn, {
    USER$logged_in <- FALSE
    USER$role <- NULL
    reset_app_state()
  })

  # Uploading a new file invalidates previously processed/joined outputs.
  observeEvent(input$csv_upload, {
    reset_app_state()
  })

  # --- 3.12 Upload Ingestion & Join-Geography Controls ---
  # Reads uploaded CSV safely and surfaces friendly file-read errors.
  uploaded_data <- reactive({
    # Return NULL until a file is provided, keeping dependent reactives quiet.
    if (is.null(input$csv_upload) || is.null(input$csv_upload$datapath)) {
      return(NULL)
    }

    tryCatch(
      read.csv(input$csv_upload$datapath, stringsAsFactors = FALSE),
      error = function(e) {
        showNotification(paste("Could not read CSV:", e$message), type = "error")
        NULL
      }
    )
  })

  # Recomputes available join geography options based on selected dataset
  # capabilities and observed GEOID/ZIP columns in current data.
  observeEvent(list(input$census_dataset, input$csv_upload), {
    req(isTRUE(USER$logged_in))

    # Re-evaluate available geography options whenever dataset/upload changes.
    # county_capable is restricted to surveys with complete county coverage;
    # ACS 1-Year/3-Year/ACSSE cover only large counties so restrict to state.
    ds <- normalize_dataset_name(input$census_dataset)
    county_capable <- !is.na(ds) && ds %in% c("acs5", "pl", "dhc", "sf1", "sf3")
    tract_capable <- !is.na(ds) && ds %in% c("acs5", "pl", "dhc", "sf1", "sf3")
    zcta_capable <- !is.na(ds) && ds %in% c("acs5")

    df <- uploaded_data()
    if (exists("APP_STATE", inherits = FALSE) &&
      !is.null(APP_STATE$processed_data) &&
      is.data.frame(APP_STATE$processed_data)) {
      df <- APP_STATE$processed_data
    }
    has_geoid <- FALSE
    has_zip <- FALSE

    if (!is.null(df) && is.data.frame(df)) {
      geoid_col <- names(df)[tolower(names(df)) == "geoid"]
      has_geoid <- length(geoid_col) >= 1

      zip_col <- names(df)[tolower(names(df)) %in% c("zip", "zipcode", "zip_code")]
      if (length(zip_col) >= 1) {
        zip_values <- unique(
          substr(gsub("[^0-9]", "", as.character(df[[zip_col[1]]])), 1, 5)
        )
        zip_values <- zip_values[nchar(zip_values) == 5]
        has_zip <- length(zip_values) > 0
      }
    }

    # Build the geography dropdown options dynamically from capabilities.
    choices <- c("Auto (recommended)" = "auto")
    if (tract_capable && has_geoid) {
      choices <- c(choices, "Tract" = "tract")
    }
    if (county_capable && has_geoid) {
      choices <- c(choices, "County" = "county")
    }
    if (zcta_capable && has_zip) {
      choices <- c(choices, "ZIP Code (ZCTA)" = "zcta")
    }
    choices <- c(choices, "State" = "state")

    # Preserve current selection when still valid; otherwise fall back to auto.
    current_choice <- isolate(as.character(input$join_geography))
    if (length(current_choice) != 1 || !(current_choice %in% unname(choices))) {
      current_choice <- "auto"
    }

    freezeReactiveValue(input, "join_geography")
    updateSelectInput(
      session,
      "join_geography",
      choices = choices,
      selected = current_choice
    )
  }, ignoreInit = FALSE)

  # --- 3.13 Preview + Error Report Outputs ---
  # Chooses preview source with precedence: selection > joined > processed > raw upload.
  preview_data <- reactive({
    # Priority order keeps user intent explicit (selection preview first,
    # then joined output, then processed rows, then raw upload).
    if (!is.null(APP_STATE$preview_selection)) {
      return(APP_STATE$preview_selection)
    }

    if (!is.null(APP_STATE$joined_data)) {
      return(APP_STATE$joined_data)
    }

    if (!is.null(APP_STATE$processed_data)) {
      return(APP_STATE$processed_data)
    }

    uploaded_data()
  })

  # Tabular preview always shows top rows from active preview source.
  output$data_preview <- renderTable({
    df <- preview_data()

    if (is.null(df)) {
      return(data.frame(
        `Data Preview` = "Upload a CSV file to preview the first 5 rows."
      ))
    }

    if (nrow(df) == 0) {
      return(data.frame(`Data Preview` = "The uploaded CSV has no rows."))
    }

    # Preview intentionally limited to first rows for responsiveness.
    head(df, 5)
  })

  # Human-readable summary describing current preview source and dimensions.
  output$preview_summary <- renderText({
    df <- preview_data()

    if (is.null(df)) {
      return("Preview Summary: Rows 0 | Columns 0 | Source: none")
    }

    source_label <- if (!is.null(APP_STATE$preview_selection)) {
      "Census selection"
    } else if (!is.null(APP_STATE$joined_data)) {
      "Joined output"
    } else if (!is.null(APP_STATE$processed_data)) {
      "Processed/Cleaned data"
    } else {
      "Uploaded data"
    }

    paste0(
      "Preview Summary: Rows ",
      nrow(df),
      " | Columns ",
      ncol(df),
      " | Source: ",
      source_label
    )
  })

  # Baseline instructional status text shown until actions update it.
  output$status_message <- renderText({
    "Upload a CSV, select Census variables, then run the join."
  })

  # Error table and downloadable CSV for rows rejected by address cleaning.
  output$error_table <- renderTable({
    req(APP_STATE$error_data)
    APP_STATE$error_data
  })

  # Download handler for current invalid-row report.
  output$download_errors <- downloadHandler(
    filename = function() {
      paste("Error_Report_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(APP_STATE$error_data, file, row.names = FALSE)
    }
  )

  # --- 3.14 Process & Persist Uploaded Data ---
  # Optional cleaning + SQLite persistence of uploaded records.
  observeEvent(input$process_btn, {
    req(uploaded_data())
    df <- uploaded_data()

    # Entering process mode clears any previous variable preview snapshot.
    APP_STATE$preview_selection <- NULL

    withProgress(message = "Processing uploaded data...", value = 0, min = 0, max = 1, {
      if (input$auto_clean) {
        setProgress(0.2, detail = "Cleaning and validating addresses...")
        # Clean and split rows into matched + error buckets.
        results <- clean_addresses(df)
        df_to_save <- results$good_data
        APP_STATE$error_data <- results$bad_data
        APP_STATE$processed_data <- df_to_save

        showNotification(
          paste(
            "Checked! Found",
            nrow(df_to_save),
            "matches and",
            nrow(results$bad_data),
            "errors."
          ),
          type = "message"
        )
      } else {
        setProgress(0.2, detail = "Preparing data...")
        # Pass-through mode stores uploaded rows without address validation.
        df_to_save <- df
        APP_STATE$error_data <- NULL
        APP_STATE$processed_data <- df_to_save
      }

      setProgress(0.7, detail = "Saving to database...")
      tryCatch({
        # Persist processed rows to SQLite under a sanitized table name.
        db_path <- censusight_data_path("my_app_database.sqlite")
        con <- dbConnect(RSQLite::SQLite(), db_path)

        raw_filename <- input$csv_upload$name
        clean_table_name <- gsub(
          "[^A-Za-z0-9_]",
          "_",
          gsub("\\.csv$", "", raw_filename, ignore.case = TRUE)
        )

        dbWriteTable(con, clean_table_name, df_to_save, overwrite = TRUE)
        dbDisconnect(con)

        setProgress(1.0, detail = "Complete!")
        output$status_message <- renderText(
          paste("Success! Valid data stored in table:", clean_table_name)
        )
      }, error = function(e) {
        showNotification(paste("Database error:", e$message), type = "error")
      })
    })
  })

  # --- 3.15 Variable Selection Preview ---
  # Builds a metadata preview table for selected variables.
  observeEvent(input$preview_btn, {
    req(input$census_year, input$census_dataset)

    withProgress(message = "Building variable preview...", value = 0.3, min = 0, max = 1, {
      selected_vars <- unique(input$census_vars)

      # Empty selection produces an instructional preview row.
      if (length(selected_vars) == 0) {
        APP_STATE$preview_selection <- data.frame(
          Note = "No Census variables selected. Choose variable(s), then click Preview.",
          stringsAsFactors = FALSE
        )
        output$status_message <- renderText(
          "Preview shows current Census selection. No variables selected yet."
        )
        return()
      }

      setProgress(0.6, detail = "Fetching variable metadata...")
      # Build metadata-only preview table for currently selected variables.
      vars <- var_list()
      selected_meta <- vars %>%
        dplyr::filter(.data$name %in% selected_vars) %>%
        dplyr::select(dplyr::all_of(c("name", "label", "concept")))

      if (nrow(selected_meta) == 0) {
        selected_meta <- data.frame(
          name = selected_vars,
          label = NA_character_,
          concept = NA_character_,
          stringsAsFactors = FALSE
        )
      }

      APP_STATE$preview_selection <- selected_meta %>%
        dplyr::mutate(
          year = as.character(input$census_year),
          dataset = as.character(input$census_dataset)
        ) %>%
        dplyr::select(year, dataset, name, label, concept)

      setProgress(1.0)
      output$status_message <- renderText(
        paste0(
          "Previewing ",
          nrow(APP_STATE$preview_selection),
          " selected Census variable(s)."
        )
      )
    })
  })

  # --- 3.16 Main Census Join Pipeline ---
  # End-to-end workflow:
  # 1) Pre-clean and GEOID-enrich upload
  # 2) Validate variable metadata and geography compatibility
  # 3) Fetch Census data with fallback logic
  # 4) Join at tract/county/zcta/state as available
  # 5) Publish joined results and status messaging
  observeEvent(input$run_join, {
    req(uploaded_data())

    # Clear selection preview because this action transitions to join output.
    df <- uploaded_data()
    APP_STATE$preview_selection <- NULL

    join_prog <- shiny::Progress$new(session, min = 0, max = 1)
    on.exit(join_prog$close(), add = TRUE)
    join_prog$set(message = "Running Census join...", value = 0.05, detail = "Pre-cleaning addresses...")

    # Normalize input fields and enrich GEOIDs from local lookup/geocoder.
    df <- preclean_for_join(df)

    join_prog$set(value = 0.15, detail = "Looking up GEOIDs...")
    geoid_enrichment <- attach_geoid_from_lookup(
      df,
      address_lookup_db_path,
      geocode_limit_per_run
    )
    df <- geoid_enrichment$data

    if (isTRUE(geoid_enrichment$used_lookup)) {
      showNotification(geoid_enrichment$message, type = "message")
    } else if (nzchar(geoid_enrichment$message)) {
      showNotification(geoid_enrichment$message, type = "warning")
    }

    join_prog$set(value = 0.25, detail = "Validating Census variables...")
    selected_vars <- unique(as.character(input$census_vars))
    selected_vars <- selected_vars[nzchar(selected_vars)]
    vars_metadata <- ensure_variable_columns(var_list())

    # Hard-stop if metadata is unavailable for selected year/dataset.
    if (!is.data.frame(vars_metadata) || nrow(vars_metadata) == 0) {
      showNotification(
        paste0(
          "No variable metadata is available for the selected dataset/year. ",
          "Choose a different survey or year."
        ),
        type = "warning",
        duration = 8
      )
      output$status_message <- renderText(
        "Join skipped: selected dataset/year has no usable variable metadata."
      )
      return()
    }

    # Enforce at least one selected variable before attempting API pull.
    if (length(selected_vars) == 0) {
      showNotification(
        "Select at least one Census variable before running the join.",
        type = "warning"
      )
      return()
    }

    # Remove variables incompatible with Wisconsin workflow constraints.
    selected_meta <- vars_metadata %>%
      dplyr::filter(.data$name %in% selected_vars) %>%
      dplyr::distinct(.data$name, .keep_all = TRUE)

    if (nrow(selected_meta) > 0) {
      state_ok <- mapply(
        variable_allowed_for_state,
        selected_meta$label,
        selected_meta$concept,
        MoreArgs = list(state_abbr = "WI")
      )

      allowed_names <- selected_meta$name[state_ok]
      removed_count <- length(setdiff(selected_vars, allowed_names))
      selected_vars <- intersect(selected_vars, allowed_names)

      if (removed_count > 0) {
        showNotification(
          paste0(
            removed_count,
            " Puerto Rico-only variable(s) were removed for Wisconsin joins."
          ),
          type = "warning",
          duration = 7
        )
      }
    }

    if (length(selected_vars) == 0) {
      showNotification(
        paste0(
          "Selected variable(s) are not available for Wisconsin geography. ",
          "Choose non-PR variables."
        ),
        type = "warning",
        duration = 8
      )
      output$status_message <- renderText(
        "Join skipped: selected variables are incompatible with Wisconsin geography."
      )
      return()
    }

    # Validate that year and dataset are usable for supported pull functions.
    year_raw <- suppressWarnings(as.integer(input$census_year))
    ds <- normalize_dataset_name(input$census_dataset)

    if (length(year_raw) != 1 || is.na(year_raw) || is.na(ds)) {
      showNotification(
        "This dataset/year combination is not supported for the join action.",
        type = "warning"
      )
      output$status_message <- renderText(
        "Join skipped: selected dataset/year is not supported."
      )
      return()
    }

    # Discover available join keys from upload data (GEOID and ZIP). 
    geoid_col <- names(df)[tolower(names(df)) == "geoid"]
    has_geoid <- length(geoid_col) >= 1
    zip_col <- names(df)[tolower(names(df)) %in% c("zip", "zipcode", "zip_code")]
    has_zip <- length(zip_col) >= 1

    zip_values <- character(0)
    if (has_zip) {
      zip_values <- unique(
        substr(gsub("[^0-9]", "", as.character(df[[zip_col[1]]])), 1, 5)
      )
      zip_values <- zip_values[nchar(zip_values) == 5]
      has_zip <- length(zip_values) > 0
    }

    showNotification("Pulling Census data and running join...", type = "message")

    # Determine effective geography from dataset capability + upload columns,
    # then optionally apply user override with guardrail notifications.
    # county_capable restricted to surveys with complete coverage (same logic
    # as the join_geography dropdown and variable observer above).
    county_capable <- !is.na(ds) && ds %in% c("acs5", "pl", "dhc", "sf1", "sf3")
    tract_capable <- ds %in% c("acs5", "pl", "dhc", "sf1", "sf3")
    zcta_capable <- ds %in% c("acs5")
    auto_preferred_geography <- if (has_geoid && tract_capable) {
      "tract"
    } else if (has_geoid && county_capable) {
      "county"
    } else if (has_zip && zcta_capable) {
      "zcta"
    } else {
      "state"
    }

    requested_geography <- as.character(input$join_geography)
    if (length(requested_geography) != 1 ||
      !requested_geography %in% c("auto", "tract", "county", "zcta", "state")) {
      requested_geography <- "auto"
    }

    active_geography <- auto_preferred_geography

    if (requested_geography != "auto") {
      if (requested_geography == "tract" && !has_geoid) {
        showNotification(
          "Tract join selected, but upload has no GEOID column; using automatic geography.",
          type = "warning",
          duration = 8
        )
      } else if (requested_geography == "tract" && !tract_capable) {
        showNotification(
          paste0(
            "Tract join selected, but this dataset does not support tract pull; ",
            "using automatic geography."
          ),
          type = "warning",
          duration = 8
        )
      } else if (requested_geography == "county" && !has_geoid) {
        showNotification(
          "County join selected, but upload has no GEOID column; using automatic geography.",
          type = "warning",
          duration = 8
        )
      } else if (requested_geography == "county" && !county_capable) {
        showNotification(
          paste0(
            "County join selected, but this dataset does not support county pull; ",
            "using automatic geography."
          ),
          type = "warning",
          duration = 8
        )
      } else if (requested_geography == "zcta" && !has_zip) {
        showNotification(
          paste0(
            "ZIP/ZCTA join selected, but upload has no valid ZIP column; ",
            "using automatic geography."
          ),
          type = "warning",
          duration = 8
        )
      } else if (requested_geography == "zcta" && !zcta_capable) {
        showNotification(
          paste0(
            "ZIP/ZCTA join selected, but this dataset does not support ZCTA pull; ",
            "using automatic geography."
          ),
          type = "warning",
          duration = 8
        )
      } else {
        active_geography <- requested_geography
      }
    }

    if (requested_geography == "auto" &&
      has_geoid &&
      !active_geography %in% c("tract", "county")) {
      showNotification(
        paste0(
          "Auto geography selected: this dataset does not support tract/county pull, ",
          "so a broader level will be used."
        ),
        type = "message",
        duration = 8
      )
    }

    # Keep only variables available at chosen geography.
    availability <- vapply(
      vars_metadata$geography,
      variable_available_for_geography,
      FUN.VALUE = logical(1),
      target_geography = active_geography
    )
    available_names <- vars_metadata$name[availability]
    requested_count <- length(selected_vars)
    selected_vars <- intersect(selected_vars, available_names)

    # If none are available, try an automatic state-level fallback.
    if (length(selected_vars) == 0 && !identical(active_geography, "state")) {
      state_availability <- vapply(
        vars_metadata$geography,
        variable_available_for_geography,
        FUN.VALUE = logical(1),
        target_geography = "state"
      )
      state_available_names <- vars_metadata$name[state_availability]
      state_selected_vars <- intersect(
        unique(as.character(input$census_vars)),
        state_available_names
      )

      if (length(state_selected_vars) > 0) {
        selected_vars <- state_selected_vars
        active_geography <- "state"
        showNotification(
          paste0(
            "Selected variables were unavailable at the requested geography; ",
            "automatically falling back to state-level join."
          ),
          type = "message",
          duration = 8
        )
      }
    }

    if (length(selected_vars) == 0) {
      showNotification(
        paste0(
          "None of the selected variables are available for the active join geography. ",
          "Choose different variable(s) or geography."
        ),
        type = "warning",
        duration = 8
      )
      output$status_message <- renderText(
        "Join skipped: selected variables are unavailable for the active join geography."
      )
      return()
    }

    if (length(selected_vars) < requested_count) {
      showNotification(
        paste0(
          requested_count - length(selected_vars),
          " selected variable(s) were skipped because they are unavailable for the active join geography."
        ),
        type = "warning",
        duration = 8
      )
    }

    # Cap selected variables for responsiveness and API payload size.
    if (length(selected_vars) > 5) {
      selected_vars <- selected_vars[1:5]
      showNotification(
        "Using the first 5 selected variables to keep the join fast.",
        type = "message"
      )
    }

    # Prepare friendly output headers before join output is rendered.
    readable_name_map <- build_readable_census_name_map(selected_vars, vars_metadata)

    join_prog$set(value = 0.4, detail = "Fetching Census data from API...")
    # Internal fetch wrapper centralizes ACS vs decennial branching.
    fetch_census <- function(
      geography_choice,
      vars,
      year_val,
      dataset_val,
      zips,
      api_counter_func
    ) {
      api_counter_func(1L)

      if (grepl("^acs", dataset_val)) {
        if (identical(geography_choice, "zcta")) {
          return(get_acs(
            geography = "zcta",
            zcta = zips,
            variables = vars,
            year = year_val,
            survey = dataset_val,
            geometry = FALSE
          ))
        }

        return(get_acs(
          geography = geography_choice,
          state = "WI",
          variables = vars,
          year = year_val,
          survey = dataset_val,
          geometry = FALSE
        ))
      }

      if (identical(geography_choice, "zcta")) {
        return(get_decennial(
          geography = "zcta",
          zcta = zips,
          variables = vars,
          year = year_val,
          sumfile = dataset_val,
          geometry = FALSE
        ))
      }

      get_decennial(
        geography = geography_choice,
        state = "WI",
        variables = vars,
        year = year_val,
        sumfile = dataset_val,
        geometry = FALSE
      )
    }

    pull_result <- tryCatch(
      list(
        data = fetch_census(
          active_geography,
          selected_vars,
          year_raw,
          ds,
          zip_values,
          increment_api_counter
        ),
        error = NULL
      ),
      error = function(e) {
        list(data = NULL, error = e$message)
      }
    )

    census_pull <- pull_result$data
    pull_error_message <- pull_result$error

    # If sub-state pull is empty, retry once at state level.
    if ((is.null(census_pull) || nrow(census_pull) == 0) &&
      !identical(active_geography, "state")) {
      active_geography <- "state"

      retry_result <- tryCatch(
        list(
          data = fetch_census(
            "state",
            selected_vars,
            year_raw,
            ds,
            zip_values,
            increment_api_counter
          ),
          error = NULL
        ),
        error = function(e) {
          list(data = NULL, error = e$message)
        }
      )

      census_pull <- retry_result$data
      pull_error_message <- retry_result$error
    }

    # If still empty, return upload rows with empty Census columns attached.
    if (is.null(census_pull) || nrow(census_pull) == 0) {
      joined_df <- df
      for (var_name in selected_vars) {
        joined_df[[paste0("census_", var_name)]] <- NA_real_
      }
      joined_df <- apply_readable_census_headers(joined_df, readable_name_map)
      APP_STATE$joined_data <- joined_df

      if (!is.null(pull_error_message) && nzchar(pull_error_message)) {
        showNotification(
          "Census API pull failed for this dataset/year. Added empty Census columns.",
          type = "warning",
          duration = 8
        )
      } else {
        showNotification(
          "No Census rows returned for the selected settings. Added empty Census columns.",
          type = "warning",
          duration = 8
        )
      }

      output$status_message <- renderText(
        paste0(
          "Join completed with empty Census values because the selected settings ",
          "returned no API rows."
        )
      )
      return()
    }

    # ACS and decennial responses use different value columns.
    join_is_acs <- grepl("^acs", ds)

    join_prog$set(value = 0.75, detail = "Joining Census data to upload...")
    # Pivot long Census response into one-row-per-GEOID wide format.
    if (join_is_acs) {
      census_wide <- census_pull %>%
        dplyr::select(dplyr::all_of(c("GEOID", "variable", "estimate"))) %>%
        dplyr::group_by(.data$GEOID, .data$variable) %>%
        dplyr::summarise(estimate = dplyr::first(.data$estimate), .groups = "drop") %>%
        tidyr::pivot_wider(
          names_from = "variable",
          values_from = "estimate",
          names_prefix = "census_"
        )
    } else {
      census_wide <- census_pull %>%
        dplyr::select(dplyr::all_of(c("GEOID", "variable", "value"))) %>%
        dplyr::group_by(.data$GEOID, .data$variable) %>%
        dplyr::summarise(value = dplyr::first(.data$value), .groups = "drop") %>%
        tidyr::pivot_wider(
          names_from = "variable",
          values_from = "value",
          names_prefix = "census_"
        )
    }

    census_wide$GEOID <- gsub("[^0-9]", "", as.character(census_wide$GEOID))

    joined_df <- df

    # Helper applies state-level fallback values to all target rows.
    apply_state_values <- function(target_df, census_df) {
      state_row <- census_df %>% dplyr::filter(.data$GEOID == "55")
      if (nrow(state_row) == 0) {
        state_row <- census_df %>% dplyr::slice(1)
      }

      if (nrow(state_row) > 0) {
        state_values <- as.list(
          state_row[1, setdiff(names(state_row), "GEOID"), drop = FALSE]
        )
        for (nm in names(state_values)) {
          target_df[[nm]] <- state_values[[nm]]
        }
      }

      target_df
    }

    # Join strategy precedence:
    # 1) tract when plausible
    # 2) county fallback
    # 3) state-value broadcast fallback
    if (has_geoid) {
      join_col <- geoid_col[1]
      joined_df$join_geoid <- gsub("[^0-9]", "", as.character(joined_df[[join_col]]))
      joined_df$join_tract <- ifelse(
        nchar(joined_df$join_geoid) >= 11,
        substr(joined_df$join_geoid, 1, 11),
        NA_character_
      )
      joined_df$join_county <- ifelse(
        nchar(joined_df$join_geoid) >= 5,
        substr(joined_df$join_geoid, 1, 5),
        NA_character_
      )

      census_geoids <- unique(
        census_wide$GEOID[!is.na(census_wide$GEOID) & nzchar(census_wide$GEOID)]
      )
      tract_like_census <- census_geoids[nchar(census_geoids) >= 11]
      county_like_census <- census_geoids[nchar(census_geoids) == 5]
      can_join_tract <- length(tract_like_census) > 0 &&
        any(joined_df$join_tract %in% tract_like_census, na.rm = TRUE)
      can_join_county <- length(county_like_census) > 0 &&
        any(joined_df$join_county %in% county_like_census, na.rm = TRUE)

      if ((identical(active_geography, "tract") && can_join_tract) ||
        (!identical(active_geography, "county") && can_join_tract)) {
        joined_df <- joined_df %>%
          dplyr::left_join(census_wide, by = c("join_tract" = "GEOID")) %>%
          dplyr::select(-dplyr::all_of(c("join_geoid", "join_tract", "join_county")))
      } else if (can_join_county) {
        joined_df <- joined_df %>%
          dplyr::left_join(census_wide, by = c("join_county" = "GEOID")) %>%
          dplyr::select(-dplyr::all_of(c("join_geoid", "join_tract", "join_county")))
      } else {
        joined_df <- apply_state_values(joined_df, census_wide) %>%
          dplyr::select(-dplyr::all_of(c("join_geoid", "join_tract", "join_county")))
        if (identical(active_geography, "state")) {
          showNotification(
            paste0(
              "Sub-state Census pull was unavailable for the selected settings; ",
              "applied state-level values to all rows."
            ),
            type = "message",
            duration = 8
          )
        } else {
          showNotification(
            paste0(
              "Census data returned at a different geography than uploaded GEOIDs; ",
              "applied state-level values to all rows."
            ),
            type = "message",
            duration = 8
          )
        }
      }
      # ZCTA join path when GEOID is unavailable but ZIP data is usable.
    } else if (has_zip && identical(active_geography, "zcta") && any(nchar(census_wide$GEOID) == 5)) {
      join_zip_col <- zip_col[1]
      joined_df$join_zcta <- substr(
        gsub("[^0-9]", "", as.character(joined_df[[join_zip_col]])),
        1,
        5
      )
      joined_df <- joined_df %>%
        dplyr::left_join(census_wide, by = c("join_zcta" = "GEOID")) %>%
        dplyr::select(-dplyr::all_of("join_zcta"))

      showNotification("No GEOID found; joined by ZIP code (ZCTA).", type = "message")
      # Final fallback: apply state values to every row.
    } else {
      joined_df <- apply_state_values(joined_df, census_wide)
      showNotification(
        "No GEOID column found in upload; applied state-level values to all rows.",
        type = "message"
      )
    }

    # Matched row count is used for post-join quality feedback.
    census_cols <- grep("^census_", names(joined_df), value = TRUE)
    matched_rows <- if (length(census_cols) == 0) {
      0
    } else {
      sum(apply(!is.na(joined_df[, census_cols, drop = FALSE]), 1, any))
    }

    if (matched_rows == 0) {
      showNotification(
        paste0(
          "Join ran but no rows matched Census values. ",
          "Check address/GEOID format and selected variables."
        ),
        type = "warning",
        duration = 8
      )
    }

    # Apply friendly header mapping and publish final joined output.
    joined_df <- apply_readable_census_headers(joined_df, readable_name_map)

    join_prog$set(value = 1.0, detail = "Complete!")
    APP_STATE$joined_data <- joined_df
    output$status_message <- renderText(
      paste0(
        "Join complete. Added ",
        ncol(joined_df) - ncol(df),
        " Census column(s) to ",
        nrow(joined_df),
        " row(s). Matched rows: ",
        matched_rows,
        "."
      )
    )
  })
}
