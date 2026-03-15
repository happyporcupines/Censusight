# -----------------------------------------------------------------------------
# File: app.R
# Purpose: Local development entry point for Censusight.
# In directory mode (shinylive / shiny::runApp()), global.R is sourced
# automatically before ui.R and server.R. This file is only used when
# launching directly via Rscript or RStudio.
# -----------------------------------------------------------------------------

# Ensure the working directory is the app directory so relative paths work.
local({
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)
  if (length(file_arg) >= 1) {
    app_dir <- normalizePath(
      dirname(sub("^--file=", "", file_arg[[1]])),
      winslash = "/", mustWork = FALSE
    )
    setwd(app_dir)
  }
})

source("global.R", local = FALSE)
source("ui.R",     local = FALSE)
source("server.R", local = FALSE)

shinyApp(ui = ui, server = server)
