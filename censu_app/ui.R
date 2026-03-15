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

ui <- page_fluid(
  theme = bs_theme(bootswatch = "zephyr", primary = "#800080"),

  # --- Busy-state overlay ---
  # When the server calls session$sendCustomMessage("set_busy", list(busy=TRUE)),
  # the JS below adds class `app-busy` to <body>.  The CSS then:
  #   1. Shows a full-viewport semi-transparent blocking layer (prevents clicks).
  #   2. Greys out the sidebar controls with reduced opacity + no-pointer cursor.
  tags$style(HTML("
    body.app-busy::before {
      content: '';
      position: fixed;
      inset: 0;
      z-index: 9998;
      background: rgba(255,255,255,0.45);
      cursor: not-allowed;
    }
    body.app-busy::after {
      content: '';
      position: fixed;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      z-index: 9999;
      width: 3rem;
      height: 3rem;
      border: 5px solid #ccc;
      border-top-color: #800080;
      border-radius: 50%;
      animation: censusight-spin 0.8s linear infinite;
    }
    @keyframes censusight-spin { to { transform: translate(-50%,-50%) rotate(360deg); } }
    body.app-busy .bslib-sidebar-layout > .sidebar,
    body.app-busy .sidebar {
      opacity: 0.45;
      pointer-events: none;
      transition: opacity 0.2s;
    }
    body.app-busy button,
    body.app-busy select,
    body.app-busy input,
    body.app-busy .selectize-control,
    body.app-busy .form-control {
      opacity: 0.5;
      pointer-events: none;
    }
  ")),

  # JS: toggles app-busy class on <body> in response to server messages.
  tags$script(HTML("
    Shiny.addCustomMessageHandler('set_busy', function(msg) {
      if (msg.busy) {
        document.body.classList.add('app-busy');
      } else {
        document.body.classList.remove('app-busy');
      }
    });
  ")),

  titlePanel("Censusight"),
  uiOutput("page_content")
)
