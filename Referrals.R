# ==============================================================================
# REFERRALS ETL SCRIPT
# ==============================================================================
# Section index:
# 0. Setup and Inputs
# 1. Master Dataset
# 2. Participant Dataset
# 3. Referral Dataset
# 4. Fact Dataset
# 5. Export

# ==============================================================================
# Session Cleanup
# ==============================================================================
reset_session_state <- function() {
  rm(list = ls(envir = .GlobalEnv), envir = .GlobalEnv)
  graphics.off()
  cat("\014")
}

reset_session_state()
# ==============================================================================

# ==============================================================================
# Package Loading
# ==============================================================================
required_packages <- c(
  "googlesheets4",
  "googledrive",
  "readxl",
  "cli",
  "stringr",
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
# Google Authentication
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
# 0. Setup and Inputs
# ==============================================================================
# (1) Target spreadsheet and destination tabs
target_gsheet <- "https://docs.google.com/spreadsheets/d/1n-DV8baXhZ5wUCYTH2psT0t65yJu8QLKCZk9aK4Eof8"
master_tab <- "Master"
part_tab <- "Part"
referral_tab <- "Referral"
fact_tab <- "Fact"
categories_tab <- "Categ_Ref"
# (2) Source files to read (link and tab name)
files_to_read <- list(
  list(
    link = "https://docs.google.com/spreadsheets/d/1mwjmN_COQsYLbrsyZESAp7Ma4LIeHudHIUkvG_dcap8",
    tab = "211 calls raw data 1-1-25 to 7-31-25"
  ),
  list(
    link = "https://docs.google.com/spreadsheets/d/1p5nhmGT4x61AY27NFrYLc11xT-GgyCmIQ5fCh-US-bo",
    tab = "All data"
  ),
  list(
    link = "https://docs.google.com/spreadsheets/d/1YEYwUeogTHiRV97hFtGu1rCKiNOl0Fbi",
    tab = "iCarolExport-CA211VenturaCounty"
  )
)

# ==============================================================================
# 0. Read Source Files
# Read each source (Google Sheets or XLSX) and store each table in `data_list`.
# ==============================================================================
data_list <- list()
accum <- 1

for (file in files_to_read) {
  link <- file$link
  tab <- file$tab
  # get file information
  file_info <- drive_get(as_id(link))
  mime <- file_info$drive_resource[[1]]$mimeType
  file_name <- file_info$name

  # dataframe name
  clean_name <- paste0("df", accum)

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
    data_list[[clean_name]] <- as.data.frame(df)
    cli_alert_success(
      paste(
        "successfully read google sheet:",
        paste0("\"", substr(file_name, 1, 5), "...\""),
        "→ tab:", paste0("\"", substr(tab, 1, 5), "...\""),
        "→ stored as:", clean_name
      )
    )

    accum <- accum + 1
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
    data_list[[clean_name]] <- as.data.frame(df)

    cli_alert_success(
      paste(
        "successfully read google sheet:",
        paste0("\"", substr(file_name, 1, 5), "...\""),
        "→ tab:", paste0("\"", substr(tab, 1, 5), "...\""),
        "→ stored as:", clean_name
      )
    )

    unlink(temp_file_path)

    accum <- accum + 1
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
  x <- trimws(x) # remove leading/trailing spaces
  x <- gsub("\uFEFF", "", x) # remove BOM
  x <- gsub("[[:cntrl:]]", "", x) # remove control/invisible chars

  return(x)
}

# remove html tags and normalize spacing in column names
clean_column_name <- function(x) {
  x <- gsub("<[^>]+>", "", x)
  x <- gsub("&nbsp;", " ", x, fixed = TRUE)
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)

  return(x)
}

# return a clean df
clean_data_frame <- function(df) {
  # clean column names
  names(df) <- clean_column_name(clean_string(names(df)))
  # clean every cell that is character
  df[] <- lapply(df, function(col) {
    if (is.character(col)) {
      return(clean_string(col))
    } else {
      return(col) # do nothing for numeric, dates, etc.
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
# normalize known column name variants before combining
normalize_column_name <- function(x) {
  x <- trimws(x)
  # remove vendor prefix present in some exports
  x <- gsub("^\\[PG&E\\]\\s*", "", x)
  # align Disaster field variant that includes "Z_"
  x <- gsub(" - Z_", " - ", x, fixed = TRUE)
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)

  return(x)
}

# explicit mapping for known "similar but not equal" columns
rename_map <- c(
  "[PG&E] PSPS Screening-Care Coordination Contact - PSPS Care Coordination-Client Name" =
    "PSPS Screening-Care Coordination Contact - PSPS Care Coordination-Client Name",
  "[PG&E] PSPS Screening-Care Coordination Contact - PSPS Care Coordination-Client Phone Number" =
    "PSPS Screening-Care Coordination Contact - PSPS Care Coordination-Client Phone Number",
  "[PG&E] PSPS Screening-Care Coordination Contact - PSPS Care Coordination-Client Time of Day" =
    "PSPS Screening-Care Coordination Contact - PSPS Care Coordination-Client Time of Day"
)

# merge columns that represent the same field after make.names() adds .1, .2 suffixes
consolidate_semantic_duplicates <- function(df) {
  base_names <- sub("\\.[0-9]+$", "", names(df))
  ordered_base_names <- unique(base_names)
  consolidated <- vector("list", length(ordered_base_names))
  names(consolidated) <- ordered_base_names

  for (i in seq_along(ordered_base_names)) {
    base_name <- ordered_base_names[i]
    idx <- which(base_names == base_name)

    if (length(idx) == 1) {
      consolidated[[i]] <- df[[idx]]
      next
    }

    merged <- df[[idx[1]]]

    for (j in idx[-1]) {
      candidate <- df[[j]]
      merged_missing <- is.na(merged) | trimws(as.character(merged)) == ""
      candidate_present <- !(is.na(candidate) | trimws(as.character(candidate)) == "")
      take_candidate <- merged_missing & candidate_present
      merged[take_candidate] <- candidate[take_candidate]
    }

    consolidated[[i]] <- merged
  }

  consolidated_df <- as.data.frame(
    consolidated,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  return(consolidated_df)
}

# prepare data to be combined
data_prepared_to_be_combined <- lapply(data_clean_ok, function(df) {
  # apply explicit rename map before normalization
  names(df) <- ifelse(
    names(df) %in% names(rename_map),
    unname(rename_map[names(df)]),
    names(df)
  )
  # normalize names after mapped replacements
  names(df) <- vapply(names(df), normalize_column_name, character(1))
  # make column names valid and unique inside each dataframe
  names(df) <- make.names(names(df), unique = TRUE)
  # convert list columns to character to avoid type conflicts in bind_rows()
  df[] <- lapply(df, function(col) {
    if (is.list(col)) as.character(col) else col
  })
  # consolidate semantic duplicates created by make.names() suffixes
  df <- consolidate_semantic_duplicates(df)

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
normalize_missing_values <- function(x) {
  x <- trimws(as.character(x))
  x[x %in% c("", "NULL", "N/A", "NA", "na", "null", "Unknown")] <- NA

  return(x)
}

standardize_title_case <- function(x) {
  x <- normalize_missing_values(x)
  x <- tolower(x)
  x <- tools::toTitleCase(x)

  return(x)
}

standardize_postal_code <- function(x) {
  x <- normalize_missing_values(x)
  x <- gsub("\\.0$", "", x)
  x <- stringr::str_extract(x, "\\b\\d{5}\\b")

  return(x)
}

standardize_language_of_call <- function(x) {
  x <- normalize_missing_values(x)
  x_lower <- tolower(x)

  x[x_lower %in% c("english", "eng", "en")] <- "English"
  x[x_lower %in% c("spanish", "espanol", "español", "spa", "es")] <- "Spanish"

  other_idx <- !is.na(x) & !(x %in% c("English", "Spanish"))
  x[other_idx] <- "Other"

  return(x)
}

standardize_contact_method <- function(x) {
  x <- normalize_missing_values(x)
  x_lower <- tolower(x)

  x[grepl("phone|call", x_lower)] <- "Phone"
  x[grepl("text|sms", x_lower)] <- "Text"
  x[grepl("email", x_lower)] <- "Email"
  x[grepl("chat", x_lower)] <- "Chat"
  x[grepl("web|online", x_lower)] <- "Web"

  return(x)
}

standardize_gender <- function(x) {
  x <- normalize_missing_values(x)
  x_lower <- tolower(x)

  x[x_lower %in% c("female", "f")] <- "Female"
  x[x_lower %in% c("male", "m")] <- "Male"
  x[grepl("trans", x_lower)] <- "Transgender"
  x[grepl("non-binary|nonbinary|genderqueer", x_lower)] <- "Non-binary"
  x[x_lower %in% c("decline to answer", "declined to answer", "prefer not to answer")] <- "Declined"
  x[x_lower %in% c("other")] <- "Other"

  return(x)
}

standardize_age <- function(x) {
  x <- normalize_missing_values(x)
  x <- suppressWarnings(as.numeric(x))
  x[x < 0 | x > 120] <- NA

  return(x)
}

standardize_master_fields <- function(df) {
  apply_if_exists <- function(data_frame, column_name, standardize_fun) {
    if (column_name %in% names(data_frame)) {
      data_frame[[column_name]] <- standardize_fun(data_frame[[column_name]])
    }

    return(data_frame)
  }

  df <- apply_if_exists(df, "CityName", standardize_title_case)
  df <- apply_if_exists(df, "CountyName", standardize_title_case)
  df <- apply_if_exists(df, "PostalCode", standardize_postal_code)
  df <- apply_if_exists(
    df,
    "Call.Information...Language.of.Call",
    standardize_language_of_call
  )
  df <- apply_if_exists(
    df,
    "Contact.Type...Contact.Method",
    standardize_contact_method
  )
  df <- apply_if_exists(df, "Demographics...Caller.Gender", standardize_gender)
  df <- apply_if_exists(df, "Demographics...Callers.Age", standardize_age)

  if ("Demographics...Caller.Age" %in% names(df)) {
    caller_age_clean <- standardize_age(df$Demographics...Caller.Age)

    if ("Demographics...Callers.Age" %in% names(df)) {
      missing_idx <- is.na(df$Demographics...Callers.Age)
      df$Demographics...Callers.Age[missing_idx] <- caller_age_clean[missing_idx]
    } else {
      df$Demographics...Callers.Age <- caller_age_clean
    }
  }

  return(df)
}

master_data_frame <- standardize_master_fields(master_data_frame)


# ==============================================================================
# 1.5 process referrals: for the referralsMade cols split multiple referrals by
# ";", extract unique values, create one column per referral,
# and mark each encounter with "x" in the appropriate referral columns
# ==============================================================================
split_referrals_made <- function(x) {
  x <- normalize_missing_values(x)
  if (is.na(x)) {
    return(character(0))
  }

  values <- unlist(strsplit(x, ";", fixed = TRUE), use.names = FALSE)
  values <- trimws(values)
  values <- values[values != ""]

  return(unique(values))
}

referrals_made_column <- "ReferralsMade"

if (referrals_made_column %in% names(master_data_frame)) {
  referral_values_per_row <- lapply(
    master_data_frame[[referrals_made_column]],
    split_referrals_made
  )
  unique_referral_values <- unique(
    unlist(referral_values_per_row, use.names = FALSE)
  )

  # create one indicator column per unique referral value
  for (referral_value in unique_referral_values) {
    if (!(referral_value %in% names(master_data_frame))) {
      master_data_frame[[referral_value]] <- ""
    }
  }

  # mark "X" in each referral indicator column for matching rows
  for (row_idx in seq_len(nrow(master_data_frame))) {
    row_referrals <- referral_values_per_row[[row_idx]]
    if (length(row_referrals) == 0) {
      next
    }

    for (referral_value in row_referrals) {
      master_data_frame[[referral_value]][row_idx] <- "X"
    }
  }
}


# ==============================================================================
# 1.6 add encounter id (EnID): sequential index within the Master table
# ==============================================================================
master_data_frame$EnID <- seq_len(nrow(master_data_frame))

# keep EnID as the first column for easier downstream joins
master_data_frame <- master_data_frame |>
  dplyr::select(EnID, dplyr::everything())

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
build_participant_data_frame <- function(master_df) {
  source_column_map <- c(
    EnID = "EnID",
    Gender = "Demographics...Caller.Gender",
    Race = "Demographics...Caller.Ethnicity",
    Age = "Demographics...Callers.Age",
    CityName = "CityName",
    CountyName = "CountyName",
    PostalCode = "PostalCode",
    `Language of Call` = "Call.Information...Language.of.Call"
  )

  participant_columns <- lapply(source_column_map, function(source_name) {
    if (source_name %in% names(master_df)) {
      return(master_df[[source_name]])
    }

    return(rep(NA, nrow(master_df)))
  })

  participant_df <- as.data.frame(
    participant_columns,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  return(participant_df)
}

participant_data_frame <- build_participant_data_frame(master_data_frame)

# ==============================================================================
# 2.2 add participant id (PartID): sequential index within the Participant table
# ==============================================================================
participant_data_frame$PartID <- seq_len(nrow(participant_data_frame))

# keep PartID as the first column in participant dataset
participant_data_frame <- participant_data_frame |>
  dplyr::select(PartID, dplyr::everything())

# ==============================================================================
# ==============================================================================
# 3. CREATE REFERRAL DATASET
# ==============================================================================
# ==============================================================================

# ==============================================================================
# 3.1 create a table with 4 columns —> EnID, RefID, Referral, and Category
# (generating one row per referral and skipping cases with no referral)
# ==============================================================================

# ==============================================================================
# for the category part:
# ==============================================================================
load_referral_category_map <- function(target_sheet_id, categories_sheet_name) {
  raw_category_map <- suppressMessages(
    googlesheets4::read_sheet(
      ss = target_sheet_id,
      sheet = categories_sheet_name
    )
  )
  raw_category_map <- as.data.frame(raw_category_map, stringsAsFactors = FALSE)

  if (all(c("Referral", "Category") %in% names(raw_category_map))) {
    category_map <- raw_category_map[, c("Referral", "Category"), drop = FALSE]
  } else {
    stacked <- stack(raw_category_map)
    names(stacked) <- c("Referral", "Category")
    category_map <- stacked
  }

  category_map <- category_map[
    !is.na(category_map$Referral) & category_map$Referral != "", ,
    drop = FALSE
  ]
  category_map <- category_map[
    !duplicated(category_map$Referral), ,
    drop = FALSE
  ]

  return(category_map)
}

referral_category_map <- load_referral_category_map(
  target_sheet_id = target_gsheet,
  categories_sheet_name = categories_tab
)

build_referral_data_frame <- function(master_df, category_map) {
  required_master_columns <- c("EnID", "ReferralsMade")
  if (!all(required_master_columns %in% names(master_df))) {
    stop("master_data_frame must include EnID and ReferralsMade columns.")
  }

  referral_rows <- lapply(seq_len(nrow(master_df)), function(row_idx) {
    referral_values <- split_referrals_made(master_df$ReferralsMade[row_idx])
    if (length(referral_values) == 0) {
      return(NULL)
    }

    data.frame(
      EnID = rep(master_df$EnID[row_idx], length(referral_values)),
      Referral = referral_values,
      stringsAsFactors = FALSE
    )
  })

  referral_df <- dplyr::bind_rows(referral_rows)

  if (nrow(referral_df) == 0) {
    empty_referral_df <- data.frame(
      EnID = integer(0),
      RefID = integer(0),
      Referral = character(0),
      Category = character(0),
      stringsAsFactors = FALSE
    )

    return(empty_referral_df)
  }

  referral_df <- referral_df |>
    dplyr::left_join(category_map, by = "Referral")

  # use explicit placeholder when category mapping is missing
  missing_category <- is.na(referral_df$Category) | referral_df$Category == ""
  referral_df$Category[missing_category] <- "--"

  referral_df$RefID <- seq_len(nrow(referral_df))

  referral_df <- referral_df |>
    dplyr::select(dplyr::all_of(c("EnID", "RefID", "Referral", "Category")))

  return(referral_df)
}

referral_data_frame <- build_referral_data_frame(
  master_df = master_data_frame,
  category_map = referral_category_map
)

# ==============================================================================
# ==============================================================================
# 4. CREATE FACT DATASET
# ==============================================================================
# ==============================================================================

# ==============================================================================
# 4.1 create a table with 6 cols -> Date, EnID, Narrative,
# ContactType, partID, refID (each referral has its own row)
# ==============================================================================
to_datetime_from_unix <- function(x) {
  x <- normalize_missing_values(x)
  numeric_value <- suppressWarnings(as.numeric(x))
  datetime_value <- rep(NA_character_, length(x))
  valid_idx <- !is.na(numeric_value)

  datetime_value[valid_idx] <- format(
    as.POSIXct(numeric_value[valid_idx], origin = "1970-01-01", tz = "UTC"),
    "%Y-%m-%d %H:%M:%S"
  )

  return(datetime_value)
}

build_fact_data_frame <- function(referral_df, master_df, participant_df) {
  if (!all(c("EnID", "RefID") %in% names(referral_df))) {
    stop("referral_data_frame must include EnID and RefID columns.")
  }

  date_source_col <- if ("CallDateAndTimeStart" %in% names(master_df)) {
    "CallDateAndTimeStart"
  } else if ("CallDateAndTimeEnd" %in% names(master_df)) {
    "CallDateAndTimeEnd"
  } else {
    NA_character_
  }

  narrative_source_col <- if ("Narrative" %in% names(master_df)) {
    "Narrative"
  } else {
    NA_character_
  }

  contact_type_source_col <- if (
    "Contact.Type...Contact.Method" %in% names(master_df)
  ) {
    "Contact.Type...Contact.Method"
  } else if ("Contact.Type...Indicate.type.of.contact" %in% names(master_df)) {
    "Contact.Type...Indicate.type.of.contact"
  } else {
    NA_character_
  }

  empty_char <- rep(NA_character_, nrow(master_df))

  if (!is.na(date_source_col)) {
    date_values <- to_datetime_from_unix(master_df[[date_source_col]])
  } else {
    date_values <- empty_char
  }

  if (!is.na(narrative_source_col)) {
    narrative_values <- normalize_missing_values(
      master_df[[narrative_source_col]]
    )
  } else {
    narrative_values <- empty_char
  }

  if (!is.na(contact_type_source_col)) {
    contact_type_values <- normalize_missing_values(
      master_df[[contact_type_source_col]]
    )
  } else {
    contact_type_values <- empty_char
  }

  master_lookup <- data.frame(
    EnID = master_df$EnID,
    Date = date_values,
    Narrative = narrative_values,
    ContactType = contact_type_values,
    stringsAsFactors = FALSE
  )

  master_lookup <- master_lookup |>
    dplyr::distinct(EnID, .keep_all = TRUE)

  participant_lookup <- participant_df |>
    dplyr::select(dplyr::all_of(c("EnID", "PartID"))) |>
    dplyr::distinct(EnID, .keep_all = TRUE)

  fact_columns <- c(
    "Date",
    "EnID",
    "Narrative",
    "ContactType",
    "PartID",
    "RefID"
  )

  fact_df <- referral_df |>
    dplyr::left_join(master_lookup, by = "EnID") |>
    dplyr::left_join(participant_lookup, by = "EnID") |>
    dplyr::select(dplyr::all_of(fact_columns))

  return(fact_df)
}

fact_data_frame <- build_fact_data_frame(
  referral_df = referral_data_frame,
  master_df = master_data_frame,
  participant_df = participant_data_frame
)

# ==============================================================================
# 5. EXPORT RESULTS to the target spreadsheet
# ==============================================================================
# write final MASTER dataset
sheet_write(
  master_data_frame,
  ss = target_gsheet,
  sheet = master_tab
)

# write final PARTICIPANT dataset
sheet_write(
  participant_data_frame,
  ss = target_gsheet,
  sheet = part_tab
)

# write final REFERRAL dataset
sheet_write(
  referral_data_frame,
  ss = target_gsheet,
  sheet = referral_tab
)

# write final FACT dataset
sheet_write(
  fact_data_frame,
  ss = target_gsheet,
  sheet = fact_tab
)

# ==============================================================================
