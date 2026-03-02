# -----------------------------------------------------------------------------
# File: app.r
# Purpose: Main Censusight application entry point.
# Responsibilities:
# - Load core package dependencies used across the Shiny app
# - Register global symbols used in non-standard evaluation pipelines
# - Configure Census API key and runtime options
# - Source helper, UI, and server scripts, then launch the app
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
  # Declare NSE symbols used by dplyr/tidyr pipelines to silence false positives.
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
# This globalVariables declaration is a workaround to suppress R CMD check notes about undefined global variables.
normalized_address <- geoid <- name <- label <- concept <- geography <- NULL
tidycensus_dataset <- vintage <- display_name <- title <- y <- NULL
GEOID <- variable <- estimate <- value <- NULL
join_geoid <- join_tract <- join_county <- join_zcta <- NULL

# Set the Census API key at startup so downstream requests can authenticate.
census_api_key("c15b342da84bf3a9879f90762b6239a8bca4085c")
# Also check for an API key in options() to allow users to set it without environment variables if preferred.
if (!nzchar(Sys.getenv("CENSUS_API_KEY"))) {
  opt_key <- getOption("census_api_key")
  if (!is.null(opt_key) && nzchar(opt_key)) {
    Sys.setenv(CENSUS_API_KEY = opt_key)
  }
}
# Set a default timeout for API requests to avoid hanging the app indefinitely on network issues.
options(shiny.launch.browser = TRUE)

# Source application modules in dependency order: helpers, UI, then server.
source("scripts/R/helpers.R", local = FALSE)
source("scripts/R/ui.R", local = FALSE)
source("scripts/R/server.R", local = FALSE)
# Finally, launch the Shiny app with the defined UI and server components.
shinyApp(
  ui = ui,
  server = server,
  options = list(
    launch.browser = TRUE
  )
)

# nolint end
