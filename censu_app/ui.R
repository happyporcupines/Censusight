# -----------------------------------------------------------------------------
# File: scripts/R/ui.R
# Purpose: Define the Censusight Shiny user interface layout and components.
# Responsibilities:
# - Configure app theme and top-level page structure
# - Expose dynamic UI placeholders rendered by server state
# - Define reusable UI bindings (such as progress bar aliases)
# -----------------------------------------------------------------------------

# --- 2. UI Definition ---

# Alias for progress gauge widget used by server updates.
progress_bar <- shinyWidgets::progressBar

# Top-level app shell:
# - Applies the shared visual theme
# - Sets the page title
# - Delegates page body rendering to `output$page_content`
ui <- page_fluid(
  theme = bs_theme(bootswatch = "zephyr", primary = "#800080"),
  titlePanel("Censusight"),
  uiOutput("page_content")
)
