# -----------------------------------------------------------------------------
# File: global.R
# Purpose: Global initialization for Censusight (multi-file Shiny app mode).
# Runs before ui.R and server.R in directory-based Shiny apps (including
# shinylive/webR). Loads all packages and defines shared helpers so they are
# available in the search path when ui.R is evaluated.
# -----------------------------------------------------------------------------

# nolint start: object_usage_linter

library(shiny)
library(DBI)
library(RSQLite)
library(bslib)
library(shinyWidgets)
library(tidycensus)
library(sf)
library(dplyr)
library(httr)

if (getRversion() >= "2.15.1") {
  utils::globalVariables(c(
    ".",
    "GEOID",
    "address",
    "cleaned_address",
    "concept",
    "dataset",
    "display_name",
    "display_with_levels",
    "estimate",
    "geography",
    "geoid",
    "geoid_lookup",
    "join_county",
    "join_geoid",
    "join_tract",
    "join_zcta",
    "name",
    "n",
    "normalized_address",
    "source",
    "tidycensus_dataset",
    "title",
    "value",
    "variable",
    "vintage",
    "year",
    "y"
  ))
}

normalized_address <- geoid <- name <- label <- concept <- geography <- NULL
tidycensus_dataset <- vintage <- display_name <- title <- y <- NULL
GEOID <- variable <- estimate <- value <- NULL
join_geoid <- join_tract <- join_county <- join_zcta <- NULL

# Set Census API key via environment variable (safe in all runtime contexts).
Sys.setenv(CENSUS_API_KEY = "c15b342da84bf3a9879f90762b6239a8bca4085c")

# Resolve the app directory. When Shiny sources global.R in directory mode,
# the working directory is already set to the app directory.
censusight_app_dir <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
censusight_project_dir <- normalizePath(
  file.path(censusight_app_dir, ".."),
  winslash = "/",
  mustWork = FALSE
)

options(
  censusight.app_dir = censusight_app_dir,
  censusight.project_dir = censusight_project_dir
)

censusight_data_path <- function(..., must_exist = FALSE) {
  app_dir <- getOption("censusight.app_dir", default = getwd())
  project_dir <- getOption("censusight.project_dir", default = dirname(app_dir))

  candidates <- c(
    file.path(app_dir, "data", ...),
    file.path(project_dir, "data", ...),
    file.path("data", ...)
  )

  if (must_exist) {
    for (candidate in candidates) {
      if (file.exists(candidate)) {
        return(candidate)
      }
    }
  }

  candidates[[1]]
}

# Source helpers before ui.R and server.R are loaded.
# When running in Shinylive/WebR the SQLite lookup database cannot be bundled
# inside app.json (202 MB). Instead, it is served as a static asset alongside
# the site and downloaded into the virtual filesystem on first startup.
local({
  target <- file.path(censusight_app_dir, "data", "address_geoid.sqlite")
  is_webr <- startsWith(
    normalizePath(getwd(), winslash = "/", mustWork = FALSE),
    "/home/web_user"
  )
  if (is_webr && !file.exists(target)) {
    sqlite_url <- getOption(
      "censusight.sqlite_url",
      "https://happyporcupines.github.io/Censusight/data/address_geoid.sqlite"
    )
    dir.create(dirname(target), showWarnings = FALSE, recursive = TRUE)
    cat(sprintf(
      "Censusight: downloading address lookup database from %s ...\n", sqlite_url
    ))
    tryCatch(
      {
        utils::download.file(sqlite_url, target, mode = "wb", quiet = TRUE)
        cat("Censusight: lookup database ready.\n")
      },
      error = function(e) {
        message("Censusight: could not download lookup database — ", conditionMessage(e))
      }
    )
  }
})

source(file.path(censusight_app_dir, "helpers.R"), local = FALSE)

# nolint end
