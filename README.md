# Censusight

Censusight (right now) is an R/Shiny app for cleaning uploaded address data, attaching/using GEOIDs, and joining Census attributes onto each row. I want to be able to make it perform quick visualizations like simple maps and charts with this joined data, as well as machine learning calculations, but currently I have to get it past this hurdle and need some help from others to see how. 

The app is currently in a state where it's working *sometimes*. I still haven't figured out the pattern of which census variable combination choices don't work. If a tester could help me figure out the pattern, I might be able to find a solution much faster than on my own.

I already know about some glitches --- like some variables or scale options not appearing untill a couple of minutes on the app or some not appearing untill the user has repeatedly clicked on different options. I'm open to different UI reccomendations or any advice on how to get rid of these. 

## Quick user sketch

Maya is a county health researcher who is great at community data interpretation, but not very technical. She gets a CSV of clinic and outreach addresses and needs Census context (income, age, household, insurance variables) fast for a meeting.

Without this app, she would usually have to wait on an analyst to: (1) write a script to clean inconsistent addresses, (2) pull and clean Census datasets from different years/surveys, and (3) script a GEOID/ZIP join or attempt a fragile Excel merge. That can take days and multiple back-and-forth revisions depending on the skill of her assistant analyst. 

With Censusight, Maya uploads her address file, runs cleaning, picks year/dataset/variables, previews what she selected, and clicks one join button. She can quickly export a joined table, review any bad rows, and move straight to analysis instead of coordinating custom code work.

## Repository map (what each file is for)

### Root

- `app.r`  
  Main application entrypoint. Loads libraries, sets Census API key behavior, sources app modules in order (`helpers.R` → `ui.R` → `server.R`), and launches `shinyApp()`.

- `.lintr`  
  Linting rules used for style and static checks in R files.

### `data/`

- `data/census_catalog.rds`  
  Cached dataset catalog used to populate year/dataset selectors quickly.

- `data/census_variable_catalog.rds`  
  Cached variable metadata by dataset/year to reduce repeated API pulls.

- `data/address_geoid.sqlite` *(expected/generated, not always committed)*  
  Local address → GEOID lookup database consumed during cleaning/join workflows.

### `scripts/`

- `scripts/build_address_geoid_sqlite.R`  
  Builds/updates `address_geoid.sqlite` from CSV input and optional Census geocoding for unmatched addresses.

- `scripts/import_postgres_parcels_to_lookup.R`  
  Imports parcel address/GEOID data from Postgres (via `RPostgres`) into the SQLite lookup cache.

- `scripts/import_postgres_parcels_to_lookup_psql.R`  
  Alternative Postgres importer that uses `psql` CLI calls (useful when direct R DB connectivity is constrained).

- `scripts/generate_real_test_addresses.R`  
  Produces realistic Wisconsin test CSV fixtures from lookup data.

### `scripts/R/` (Shiny app modules)

- `scripts/R/helpers.R`  
  Shared non-reactive utility logic: address normalization/validation, dataset/year/geography normalization, Census metadata handling, and join helpers.

- `scripts/R/ui.R`  
  Defines the top-level Shiny UI shell/theme and dynamic page container.

- `scripts/R/server.R`  
  Full reactive/server behavior: session state, upload processing, catalog refresh, variable selection, Census pull, joins, notifications, and outputs.

### `testing/`

- `testing/real_wi_test_addresses.csv`  
  Realistic test upload file used for manual validation of cleaning + Census join behavior.

## How files interact (runtime flow)

1. Start app through `app.r`.
2. `app.r` sources `scripts/R/helpers.R`, `scripts/R/ui.R`, then `scripts/R/server.R`.
3. `server.R` loads cached catalog data from `data/*.rds`, falls back to helper fetch/default logic if needed.
4. User uploads CSV (for example `testing/real_wi_test_addresses.csv`).
5. `helpers.R::clean_addresses()` validates/normalizes addresses and attempts local GEOID matching from `data/address_geoid.sqlite`.
6. `server.R` uses selected Census year/dataset/variables, calls Census APIs, and joins data to processed rows.
7. Results and errors are exposed in app outputs and downloadable tables.

## Data-prep script relationship

- Postgres import scripts feed the local SQLite lookup cache (`data/address_geoid.sqlite`).
- `build_address_geoid_sqlite.R` can also create/augment the same lookup cache from CSV + geocoder.
- `generate_real_test_addresses.R` can use the lookup cache to produce realistic testing fixtures.
- The app (`app.r` + `scripts/R/*`) consumes all of the above artifacts at runtime.

---
## What you need to run the app for testing
To get Censusight up and running on your local machine for testing, you’ll need to have the following ready to go:

1. R and RStudio
The app is built using the R programming language.

Install R: Download from CRAN.

Install RStudio: The best "workbench" for running this app. Download the free Desktop version from Posit.

2. Census API Key
I'm giving you the right to use the census API key currently in the code to test, but try not to max out the API call limit. Count how many calls you've used (the call counter resets when you refresh, but your technical count does not, so sum counts between refreshes). 

3. Required R Packages
Once R is installed, open RStudio and run the following command in the Console to grab all the necessary libraries:

R
install.packages(c("shiny", "DBI", "RSQLite", "bslib", "shinyWidgets", 
                   "tidycensus", "sf", "dplyr", "httr", "tidyverse"))
                   
4. System Dependencies (Spatial Data)
Because the app handles maps and GEOIDs (spatial data), your computer needs a few background drivers.

Windows: You're usually good to go!

macOS: You may need Homebrew to install gdal (the "translator" for map data) if the sf package doesn't install correctly.

Linux: Ensure libgdal-dev, libproj-dev, and libudunits2-dev are installed.


## Testing session: manual app validation using provided testing data

This is a starter test session you can run repeatedly while developing.

### 1) Pre-checks

- Ensure required R packages are installed (`shiny`, `DBI`, `RSQLite`, `bslib`, `shinyWidgets`, `tidycensus`, `sf`, `dplyr`, `httr`, plus any packages referenced in helpers/server).
- Confirm these files exist:
  - `testing/real_wi_test_addresses.csv`
  - `data/census_catalog.rds`
  - `data/census_variable_catalog.rds`
- Recommended: ensure `data/address_geoid.sqlite` exists for high match rates.

### 2) Launch the app

From repo root:

```bash
Rscript app.r
```
## (EASIEST METHOD) you can also just click the "play" button on app.r if you have R Studio installed
Alternative interactive launch:

```r
shiny::runApp(".")
```

### 3) Login/state setup

- Use the app login controls (Admin or Guest Tester).
- Verify the main upload/join interface renders after login.

### 4) Upload test CSV and run cleaning

- Upload `testing/real_wi_test_addresses.csv`.
- Run the address check/processing action.
- Expected outcomes:
  - Status message reports match/error counts.
  - Preview table shows first rows from uploaded/processed data.
  - Error report panel appears only when invalid rows exist.

### 5) Census selection and preview

- Select a year and dataset.
- Wait for census variable choices to populate.
- Select a small set of variables (start with 1–3).
- Click **Preview**.
- Expected outcomes:
  - Preview status text updates.
  - Selected variable metadata table shows year/dataset/name/label/concept.

### 6) Run spatial/Census join

- Click **Perform Spatial Join with Census Data**.
- Expected outcomes:
  - Notification appears while API pull/join runs.
  - Joined output includes new `census_*` columns.
  - If a geography fallback is needed, app should notify and still complete gracefully.

### 7) Error-path checks (quick)

- Re-run with zero variables selected and click Preview/Join.
- Expected: warning/status explaining that variables must be selected.

- Upload a CSV missing `address` column.
- Expected: clear error that `address` is required.

### 8) Optional regression checklist (start of a fuller test suite)

- [ ] App boots without parse/runtime error.
- [ ] Upload works with provided test fixture.
- [ ] Address cleaning returns deterministic row counts across repeated runs.
- [ ] Variable selector updates when year/dataset changes.
- [ ] Join adds expected Census columns and preserves original rows.
- [ ] Error download works when invalid rows exist.
- [ ] Logout/reset clears session-specific data views.

---
