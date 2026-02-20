# ==============================================================================
# Referrals - Feb 26
# ==============================================================================

# ==============================================================================
# clean around
# ==============================================================================
rm(list=ls()) # environment
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

reqpacks <- c("tidyverse",
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
for (apack in reqpacks) {
  if(!suppressWarnings(require(apack, character.only = TRUE))) {
    install.packages(apack, repos = "http://cran.us.r-project.org")
    library(apack, character.only = TRUE)
  }
}
# ==============================================================================



# ==============================================================================
# Authorization GSheets
# ==============================================================================

serviceAccountPath <- ".secrets/service_account.json"

if (file.exists(serviceAccountPath)) {
  drive_auth(path = serviceAccountPath)
  gs4_auth(path = serviceAccountPath)
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
targetGSheet <- "https://docs.google.com/spreadsheets/d/1n-DV8baXhZ5wUCYTH2psT0t65yJu8QLKCZk9aK4Eof8"
masterTab <- "Master"
partTab <- "Part"
referralTab <- "Referral"
factTab <- "Fact"
# ==============================================================================
# (2) files to read -> link and tab name
# ==============================================================================
filesToRead <- list(
  list(
    link = "https://docs.google.com/spreadsheets/d/1mwjmN_COQsYLbrsyZESAp7Ma4LIeHudHIUkvG_dcap8",
    tab  = "211 calls raw data 1-1-25 to 7-31-25"
  ),
  list(
    link = "https://docs.google.com/spreadsheets/d/1p5nhmGT4x61AY27NFrYLc11xT-GgyCmIQ5fCh-US-bo",
    tab  = "All data"
  ),
  list(
    link = "https://docs.google.com/spreadsheets/d/1YEYwUeogTHiRV97hFtGu1rCKiNOl0Fbi",
    tab  = "iCarolExport-CA211VenturaCounty"
  )
)

# ===================================================================================
# ===================================================================================
# 0. READ EACH FILE (google sheet or xlsx) and save it inside a list of dataframes
# ===================================================================================
# ===================================================================================
dataList <- list()
i <- 1

for (item in filesToRead) {
  
  link <- item$link
  tab  <- item$tab
  
  # get file information
  fileInfo <- drive_get(as_id(link))
  mime     <- fileInfo$drive_resource[[1]]$mimeType
  fileName <- fileInfo$name
  
  # dataframe name
  cleanName <- cleanName <- paste0("df", i)
  
  # -------------------------
  # google sheet files
  # -------------------------
  if (mime == "application/vnd.google-apps.spreadsheet") {
    
    df <- suppressMessages(read_sheet(as_id(link), sheet = tab, col_names = FALSE))
    
    # store dataframe into the list
    dataList[[cleanName]] <- as.data.frame(df)
    
    cli_alert_success(
      paste("successfully read google sheet:", paste0("\"", substr(fileName, 1, 5), "...\""),
        "→ tab:", paste0("\"", substr(tab, 1, 5), "...\""), "→ stored as:", cleanName)
    )
    
    i <- i + 1
    next
  }
  
  # -------------------------
  # excel files
  # -------------------------
  if (grepl("xml|sheet|excel|xlsx|xls", mime, ignore.case = TRUE)) {
    
    tempFilePath <- tempfile(fileext = ".xlsx")
    suppressMessages(drive_download(as_id(link), tempFilePath, overwrite = TRUE))
    
    df <- suppressMessages(read_excel(tempFilePath, sheet = tab, col_names = FALSE))
    
    # store dataframe into the list
    dataList[[cleanName]] <- as.data.frame(df)
    
    cli_alert_success(
      paste("successfully read google sheet:", paste0("\"", substr(fileName, 1, 5), "...\""),
            "→ tab:", paste0("\"", substr(tab, 1, 5), "...\""), "→ stored as:", cleanName)
    )
    
    unlink(tempFilePath)
    
    i <- i + 1
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
detectAndSetHeaders <- function(df) {
  # get the exact number of columns
  totalCols <- ncol(df)
  
  # count how many non-empty values exist in each row
  nonEmptyPerRow <- apply(df, 1, function(x) sum(!is.na(x) & x != ""))
  
  # find the first row that has exactly totalCols non-empty values (the header row)
  headerRow <- which(nonEmptyPerRow == totalCols)[1]
  
  # fail safely if no matching row is found
  if (is.na(headerRow)) {
    stop("no header row found with the exact expected number of columns.")
  }
  
  # extract headers
  headerNames <- as.character(unlist(df[headerRow, ], use.names = FALSE))
  names(df) <- headerNames
  
  # remove all rows before the header
  df <- df[-c(1:headerRow), ]
  
  # reset row names
  rownames(df) <- NULL
  
  return(df)
}

# apply header detection to every dataframe in the list
dataHeadersOk <- lapply(dataList, detectAndSetHeaders)
# ==============================================================================

# ==============================================================================
# 1.2 clean columns and rows (apply correct date formats)
# ==============================================================================
# function which cleans any string: trim spaces, remove BOM, remove control chars
cleanString <- function(x) {
  x <- trimws(x)                    # remove leading/trailing spaces
  x <- gsub("\uFEFF", "", x)        # remove BOM
  x <- gsub("[[:cntrl:]]", "", x)   # remove control/invisible chars
  return(x)
}

# return a clean df
cleanDataframe <- function(df) {
  # clean column names
  names(df) <- cleanString(names(df))
  
  # clean every cell that is character
  df[] <- lapply(df, function(col) {
    if (is.character(col)) {
      return(cleanString(col))
    } else {
      return(col)   # do nothing for numeric, dates, etc.
    }
  })
  
  return(df)
}

# apply dataframe cleaning to every dataframe in the list
dataCleanOk <- lapply(dataHeadersOk, cleanDataframe)
# ==============================================================================

# ==============================================================================
# 1.3 merge all individual datasets into one master dataframe
# ==============================================================================
#  prepare data to be combined
dataPrepToBeCombined <- lapply(dataCleanOk, function(df) {
  
  # make column names valid and unique inside each dataframe
  names(df) <- make.names(names(df), unique = TRUE)
  
  # convert list columns to character to avoid type conflicts in bind_rows()
  df[] <- lapply(df, function(col) {
    if (is.list(col)) as.character(col) else col
  })
  
  return(df)
})

# merge all dataframes into one master dataframe
masterDf <- bind_rows(dataPrepToBeCombined)
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
# 2.1 create a table with 8 cols -> EnID, Gender, Race, Age, CityName, CountyName,
# PostalCode, Call Information - Language of Call (retitled -> Language of Call) 
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
# 3.1 create a table with 4 columns —> EnID, refID, Referral, and Category (generating
# one row per referral and skipping cases with no referral)
# ==============================================================================

# ==============================================================================
# for the category part:
# ==============================================================================
refToCat <- c(
  
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

combinedDf$Category <- refToCat[combinedDf$Referral]



# ==============================================================================
# ==============================================================================
# 4. CREATE FACT DATASET
# ==============================================================================
# ==============================================================================

# ==============================================================================
# 4.1 create a table with 6 cols -> Date, EnID, Narrative, ContactType, partID, refID
# (each referral has its own row)
# ==============================================================================



# ==============================================================================
# 5. EXPORT RESULTS to the target spreadsheet
# ==============================================================================
# write final MASTER dataset
sheet_write(finalMasterDf, 
            ss=targetGSheet,
            sheet=masterTab)

# write final PARTICIPANT dataset
sheet_write(finalPartDf, 
            ss=targetGSheet,
            sheet=partTab)


# write final REFERRAL dataset
sheet_write(finalReferralDf, 
            ss=targetGSheet,
            sheet=referralTab)


# write final FACT dataset
sheet_write(finalFactDf, 
            ss=targetGSheet,
            sheet=factTab)

# ==============================================================================
