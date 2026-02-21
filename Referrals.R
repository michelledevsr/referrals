# ==============================================================================
# Referrals - Feb 26
# ==============================================================================

# ==============================================================================
# clean around
# ==============================================================================
rm(list = ls()) # environment
graphics.off() # plotting devices
cat("\014") # console
# ==============================================================================

# ==============================================================================
# load the necessary packages (install if necessary)
# ==============================================================================

install.packages("textshaping", type = "source")
install.packages("ragg", type = "source")
install.packages("tidyverse")
install.packages("units", type = "source")
install.packages("sf", type = "source")
install.packages("tigris")
install.packages("tidycensus")

required_packages <- c(
  "tidyverse",
  "googlesheets4",
  "googledrive",
  "tidyverse",
  "gsheet",
  "tidycensus",
  "data.table",
  "readxl",
  "janitor",
  "cli",
  "pdftools",
  "stringr",
  "purrr",
  "httr",
  "jsonlite",
  "stringi",
  "dplyr"
)

for (package in required_packages) {
  if (!suppressWarnings(require(package, character.only = TRUE))) {
    install.packages(package, repos = "http://cran.us.r-project.org")
    library(package, character.only = TRUE)
  }
}
# ==============================================================================



# ==============================================================================
# Authorization GSheets
# ==============================================================================

service_account_key_path <- ".secrets/service_account.json"

if (file.exists(service_account_key_path)) {
  drive_auth(path = service_account_key_path)
  gs4_auth(path = service_account_key_path)
} else {
  token <- gargle::token_fetch(
    scopes = c(
      "https://www.googleapis.com/auth/drive",
      "https://www.googleapis.com/auth/spreadsheets"
    )
  )

  # Use fetched token when service account file is not available
  drive_auth(token = token)
  gs4_auth(token = token)
}
# ==============================================================================



# ==============================================================================
# ==============================================================================
# ****** inputs: (1) and (2) ****
# ==============================================================================
# (1) drive link towards the target sheet
# ==============================================================================
google_spreadsheet_url <- "https://docs.google.com/spreadsheets/d/"
target_gsheet <- paste0(
  google_spreadsheet_url,
  "1n-DV8baXhZ5wUCYTH2psT0t65yJu8QLKCZk9aK4Eof8"
)
master_tab <- "Master"
part_tab <- "Part"
referral_tab <- "Referral"
fact_tab <- "Fact"
# ==============================================================================
# (2) files to read -> link and tab name
# ==============================================================================
files_to_red <- list(
  list(
    link = paste0(
      google_spreadsheet_url,
      "1mwjmN_COQsYLbrsyZESAp7Ma4LIeHudHIUkvG_dcap8"
    ),
    tab  = "211 calls raw data 1-1-25 to 7-31-25"
  ),
  list(
    link = paste0(
      google_spreadsheet_url,
      "1p5nhmGT4x61AY27NFrYLc11xT-GgyCmIQ5fCh-US-bo"
    ),
    tab  = "All data"
  ),
  list(
    link = paste0(
      google_spreadsheet_url,
      "1YEYwUeogTHiRV97hFtGu1rCKiNOl0Fbi"
    ),
    tab  = "iCarolExport-CA211VenturaCounty"
  )
)

# ==============================================================================
# ==============================================================================
# 0. READ EACH FILE (google sheet or xlsx)
# and save it inside a list of dataframes
# ==============================================================================
# ==============================================================================
data_list <- list()
accum <- 1

for (file in files_to_red) {
  link <- file$link
  tab  <- file$tab
  # get file information
  file_info <- drive_get(as_id(link))
  mime     <- file_info$drive_resource[[1]]$mimeType
  file_name <- file_info$name

  # dataframe name
  clean_name <- clean_name <- paste0("df", accum)

  # -------------------------
  # google sheet files
  # -------------------------
  if (mime == "application/vnd.google-apps.spreadsheet") {
    df <- suppressMessages(
      read_sheet(
        as_id(link),
        sheet = tab,
        col_names = FALSE
      )
    )
    # store dataframe into the list
    data_list[[cleanName]] <- as.data.frame(df)
    cli_alert_success(
      paste(
        "successfully read google sheet:",
        paste0("\"", substr(file_name, 1, 5), "...\""),
        "→ tab:", paste0("\"", substr(tab, 1, 5), "...\""),
        "→ stored as:", cleanName
      )
    )

    i <- accum + 1
    next
  }
  # -------------------------
  # excel files
  # -------------------------
  if (grepl("xml|sheet|excel|xlsx|xls", mime, ignore.case = TRUE)) {
    temp_file_path <- tempfile(fileext = ".xlsx")

    suppressMessages(
      drive_download(as_id(link), temp_file_path, overwrite = TRUE)
    )

    df <- suppressMessages(
      read_excel(temp_file_path, sheet = tab, col_names = FALSE)
    )
    # store dataframe into the list
    data_list[[cleanName]] <- as.data.frame(df)

    cli_alert_success(
      paste(
        "successfully read google sheet:", 
        paste0("\"", substr(file_name, 1, 5), "...\""),
        "→ tab:", paste0("\"", substr(tab, 1, 5), "...\""), 
        "→ stored as:", cleanName
      )
    )

    unlink(temp_file_path)

    i <- accum + 1
    next
  }

  # unrecognized type
  message("unrecognized file type: ", mime)
}
# ==============================================================================


# ==============================================================================
# ==============================================================================
# 1. CREATE MASTER DATASET
# ==============================================================================
# ==============================================================================

# ==============================================================================
# 1.1 extract the correct headers for each dataset
# ==============================================================================
# function which identifies headers and returns the df with the right headers
detect_and_set_headers <- function(df) {
  # get the exact number of columns
  total_cols <- ncol(df)
  # count how many non-empty values exist in each row
  non_empty_per_row <- apply(df, 1, function(x) sum(!is.na(x) & x != ""))
  # find the first row that has exactly total_cols non-empty values
  header_row <- which(non_empty_per_row == total_cols)[1]

  # fail safely if no matching row is found
  if (is.na(header_row)) {
    stop("no header row found with the exact expected number of columns.")
  }

  # extract headers
  header_names <- as.character(unlist(df[header_row, ], use.names = FALSE))
  names(df) <- header_names
  # remove all rows before the header
  df <- df[-c(1:header_row), ]
  # reset row names
  rownames(df) <- NULL

  return(df)
}

# apply header detection to every dataframe in the list
data_headers_ok <- lapply(data_list, detect_and_set_headers)
# ==============================================================================

# ==============================================================================
# 1.2 clean columns and rows (apply correct date formats)
# ==============================================================================
# function which cleans any string: trim spaces,
# remove BOM, remove control chars
clean_string <- function(x) {
  x <- trimws(x)                    # remove leading/trailing spaces
  x <- gsub("\uFEFF", "", x)        # remove BOM
  x <- gsub("[[:cntrl:]]", "", x)   # remove control/invisible chars

  return(x)
}

# return a clean df
clean_data_frame <- function(df) {
  # clean column names
  names(df) <- clean_string(names(df))
  # clean every cell that is character
  df[] <- lapply(df, function(col) {
    if (is.character(col)) {
      return(clean_string(col))
    } else {
      return(col)   # do nothing for numeric, dates, etc.
    }
  })

  return(df)
}

# apply dataframe cleaning to every dataframe in the list
data_clean_ok <- lapply(data_headers_ok, clean_data_frame)
# ==============================================================================

# ==============================================================================
# 1.3 merge all individual datasets into one master dataframe
# ==============================================================================
#  prepare data to be combined
data_prepared_to_be_combined <- lapply(data_clean_ok, function(df) {
  # make column names valid and unique inside each dataframe
  names(df) <- make.names(names(df), unique = TRUE)
  # convert list columns to character to avoid type conflicts in bind_rows()
  df[] <- lapply(df, function(col) {
    if (is.list(col)) as.character(col) else col
  })

  return(df)
})

# merge all dataframes into one master dataframe
master_data_frame <- bind_rows(data_prepared_to_be_combined)
# ==============================================================================


# ==============================================================================
# 1.4 standardize responses in: CityName, CountyName, PostalCode,
# Call Information - Language of Call, Contact Type - # Contact Method,
# Demographics - Caller Gender, Demographics - Callers Age)
# ==============================================================================



# ==============================================================================
# 1.5 process referrals: for the referralsMade cols split multiple referrals by
# ";", extract unique values, create one column per referral,
# and mark each encounter with "x" in the appropriate referral columns
# ==============================================================================


# ==============================================================================
# 1.6 add encounter id (EnID): sequential index within the Master table
# ==============================================================================




# ==============================================================================
# ==============================================================================
# 2. CREATE PARTICIPANT DATASET
# ==============================================================================
# ==============================================================================

# ==============================================================================
# 2.1 create a table with 8 cols -> EnID, Gender, Race,
# Age, CityName, CountyName, PostalCode,
# Call Information - Language of Call (retitled -> Language of Call)
# ==============================================================================

# ==============================================================================
# 2.2 add participant id (PartID): sequential index within the Participant table
# ==============================================================================



# ==============================================================================
# ==============================================================================
# 3. CREATE REFERRAL DATASET
# ==============================================================================
# ==============================================================================

# ==============================================================================
# 3.1 create a table with 4 columns —> EnID, refID, Referral, and Category
# (generating one row per referral and skipping cases with no referral)
# ==============================================================================

# ==============================================================================
# for the category part:
# ==============================================================================
ref_to_category <- c(
  # Housing & Homelessness Services
  "Affordable Housing" = "Housing & Homelessness Services",
  "Highlands Village Senior Apartments" = "Housing & Homelessness Services",
  "Homeless and Housing Services" = "Housing & Homelessness Services",
  "Homeless Owner Pet Care" = "Housing & Homelessness Services",
  "Homeless Shelter" = "Housing & Homelessness Services",
  "Housing Assistance Program" = "Housing & Homelessness Services",
  "Housing Choice Voucher Rental Assistance Program" = "Housing & Homelessness Services",
  "Low-income/Subsidized Housing" = "Housing & Homelessness Services",
  "Minor Home Repair Program" = "Housing & Homelessness Services",
  "Home Repair Program" = "Housing & Homelessness Services",
  "Mobilehome Assistance Center" = "Housing & Homelessness Services",
  "Familiar Faces Homeless Outreach" = "Housing & Homelessness Services",
  "Fisher House" = "Housing & Homelessness Services",
  
  # Utility & Financial Assistance
  "Utility Assistance" = "Utility & Financial Assistance",
  "Low Income Home Energy Assistance Program (LIHEAP)" = "Utility & Financial Assistance",
  "Weatherization Program" = "Utility & Financial Assistance",
  "Safety Net Program/The Storm Inconvenience Payment" = "Utility & Financial Assistance",
  "Self-Sufficiency Grant" = "Utility & Financial Assistance",
  "General Assistance" = "Utility & Financial Assistance",
  "CalWORKS" = "Utility & Financial Assistance",
  "PG&E CARE/FERA Energy Discount Program" = "Utility & Financial Assistance",
  "PG&E General Power Outage Information" = "Utility & Financial Assistance",
  "SCE Energy Payment Assistance and Billing Plans" = "Utility & Financial Assistance",
  "California LifeLine Telephone/Wireless Programs" = "Utility & Financial Assistance",
  "Access Low Cost Internet" = "Utility & Financial Assistance",
  "Spectrum Internet Assist" = "Utility & Financial Assistance",
  
  # Crisis, Safety & Navigation Services
  "2-1-1 ALAMEDA COUNTY (CALIFORNIA)" = "Crisis, Safety & Navigation Services",
  "2-1-1 FINGER LAKES REGION (NEW YORK)" = "Crisis, Safety & Navigation Services",
  "2-1-1 ORANGE COUNTY (CALIFORNIA)" = "Crisis, Safety & Navigation Services",
  "2-1-1 STANISLAUS COUNTY (CALIFORNIA)" = "Crisis, Safety & Navigation Services",
  "2-1-1 VENTURA (CALIFORNIA)" = "Crisis, Safety & Navigation Services",
  "211 SLO COUNTY (CALIFORNIA)" = "Crisis, Safety & Navigation Services",
  "211.org" = "Crisis, Safety & Navigation Services",
  "EASTERN WASHINGTON 2-1-1" = "Crisis, Safety & Navigation Services",
  "MOUNTAIN VALLEY 211 (CALIFORNIA)" = "Crisis, Safety & Navigation Services",
  "24/7 Crisis Line" = "Crisis, Safety & Navigation Services",
  "988 Suicide and Crisis Lifeline" = "Crisis, Safety & Navigation Services",
  "995 HOPE Hotline" = "Crisis, Safety & Navigation Services",
  "National Homeless Youth Crisis Hotline" = "Crisis, Safety & Navigation Services",
  "Safe Place" = "Crisis, Safety & Navigation Services",
  "Victims of Crime Resource Center" = "Crisis, Safety & Navigation Services",
  "Child Abuse Reporting" = "Crisis, Safety & Navigation Services",
  "Police Services" = "Crisis, Safety & Navigation Services",
  "Sheriff Department" = "Crisis, Safety & Navigation Services",
  "Clear Lake Area CHP" = "Crisis, Safety & Navigation Services",
  "City of Stockton Animal Services Unit" = "Crisis, Safety & Navigation Services",
  "Emergency and Disaster Information" = "Crisis, Safety & Navigation Services",
  "Animal Poison Control Hotline" = "Crisis, Safety & Navigation Services",
  "Social Services" = "Crisis, Safety & Navigation Services",
  "Community Center" = "Crisis, Safety & Navigation Services",
  "Community Support Services (CSS)" = "Crisis, Safety & Navigation Services",
  "Enrichment Center" = "Crisis, Safety & Navigation Services",
  
  # Aging, Disability & Caregiving Services
  "Area Agency on Aging" = "Aging, Disability & Caregiving Services",
  "Area 12 Agency on Aging" = "Aging, Disability & Caregiving Services",
  "Department of Aging" = "Aging, Disability & Caregiving Services",
  "Dial-A-Ride" = "Aging, Disability & Caregiving Services",
  "Disability Disaster Access and Resources (DDAR) Program" = "Aging, Disability & Caregiving Services",
  "Disability Resource Services" = "Aging, Disability & Caregiving Services",
  "In-Home Support Program" = "Aging, Disability & Caregiving Services",
  "In-Home Supportive Services (IHSS)" = "Aging, Disability & Caregiving Services",
  "Home Caregiver Services" = "Aging, Disability & Caregiving Services",
  "Adult Protective Services" = "Aging, Disability & Caregiving Services",
  "Family Caregiver Support Program" = "Aging, Disability & Caregiving Services",
  "Health Insurance Counseling & Advocacy Program (HICAP)" = "Aging, Disability & Caregiving Services",
  "Medicare Helpline" = "Aging, Disability & Caregiving Services",
  "VetFam" = "Aging, Disability & Caregiving Services",
  
  # Food Assistance
  "CalFresh" = "Food Assistance",
  "All Saints Catholic Parish Food Pantry" = "Food Assistance",
  "Christian Heights Assembly of God Church Food Pantry" = "Food Assistance",
  "Groveland Evangelical Free Church Food Pantry" = "Food Assistance",
  "Interfaith Food Pantry" = "Food Assistance",
  "Lighthouse Ministries Food Pantry" = "Food Assistance",
  "Mary Laveroni Community Park Food Pantry" = "Food Assistance",
  "Nancys Hope Community Center Food Pantry" = "Food Assistance",
  "Sierra Bible Church Food Pantry" = "Food Assistance",
  "Tuolumne United Methodist Church Food Pantry" = "Food Assistance",
  "Holiday Food Baskets" = "Food Assistance",
  "Adopt a Family" = "Food Assistance",
  
  # Health & Behavioral Health
  "Addiction Support Services" = "Health & Behavioral Health",
  "Alcohol/Substance Abuse Treatment Program" = "Health & Behavioral Health",
  "Substance Addiction Education and Prevention" = "Health & Behavioral Health",
  "Behavioral Health" = "Health & Behavioral Health",
  "Therapy Referrals" = "Health & Behavioral Health",
  "Use Disorder and Mental Health Assistance" = "Health & Behavioral Health",
  "Medi-Cal" = "Health & Behavioral Health",
  "California Advancing and Innovating Medi-Cal (CalAIM)" = "Health & Behavioral Health",
  "Medical Education and Referrals" = "Health & Behavioral Health",
  "Mathiesen Memorial Health Clinic" = "Health & Behavioral Health",
  "Home Health Care" = "Health & Behavioral Health",
  "Immunization Clinic" = "Health & Behavioral Health",
  "Crisis Assessment & Intervention Program" = "Health & Behavioral Health",
  "Road to Recovery Cancer Patient Services" = "Health & Behavioral Health",
  "Family PACT" = "Health & Behavioral Health",
  "National Prevention Information Network (NPIN)" = "Health & Behavioral Health",
  
  # Legal Services
  "Legal Services" = "Legal Services",
  "Legal Aid Services" = "Legal Services",
  "Native American Legal Services" = "Legal Services",
  "Self-Help Center Website" = "Legal Services",
  "Superior Court Self Help Center" = "Legal Services",
  "Submit a Complaint" = "Legal Services",
  "Tenant Rights Hotline" = "Legal Services",
  "Tenant Protection" = "Legal Services",
  "Tenant Power Toolkit" = "Legal Services",
  
  # Education & Employment Support
  "Adult Literacy Program" = "Education & Employment Support",
  "Mother Lode Job Training Services" = "Education & Employment Support",
  "Child Care Resource and Referral" = "Education & Employment Support"
  
)

combinedDf$Category <- ref_to_category[combinedDf$Referral]



# ==============================================================================
# ==============================================================================
# 4. CREATE FACT DATASET
# ==============================================================================
# ==============================================================================

# ==============================================================================
# 4.1 create a table with 6 cols -> Date, EnID, Narrative,
# ContactType, partID, refID (each referral has its own row)
# ==============================================================================



# ==============================================================================
# 5. EXPORT RESULTS to the target spreadsheet
# ==============================================================================
# write final MASTER dataset
sheet_write(finalMasterDf, 
            ss=target_gsheet,
            sheet=master_tab)

# write final PARTICIPANT dataset
sheet_write(finalPartDf, 
            ss=target_gsheet,
            sheet=part_tab)


# write final REFERRAL dataset
sheet_write(finalReferralDf, 
            ss=target_gsheet,
            sheet=referral_tab)


# write final FACT dataset
sheet_write(finalFactDf, 
            ss=target_gsheet,
            sheet=fact_tab)

# ==============================================================================
