#(NORDSALT Experiment) processing scripts
## Adapted for NORDSALT by Liz Westbrook 04/2025. Original scripts created for porcessing MERIT data by Selina Cheng December 2022. 

# This script runs functions to clean SD data that were filled with loggernet data by:
# 1) Using range limitation functions to roughly clean the data
# 2) Use rolling SDs to further clean the data
# 3) Create derived variables (deltas) for later use

# This script runs all the functions that were created in "3-cleaning-functions.R"

## ================================= Load functions ============================================
# Source functions and packages from "3-cleaning-functions.R"
source("scripts/2-cleaning-functions.R")

mid_dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA")

# Set SD dir to get data
norm_dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/")

# ============================== Step 1: Clean data using rolling standard deviations ============
# 1) Create path to directories
source_dir <- file.path(norm_dir, "3_normal/norm_working")
output_dir <- file.path(norm_dir, "4_cleaned/clean_working")

# Run function that cleans data
# 1) Using coarse range limitation functions
# 2) Using narrower rolling standard deviation functions
year <- "2022"
clean_data(source_dir, output_dir, year)

#put files from all 3 pools together
library(dplyr)

excel_files <- list.files(paste0(mid_dir,"/4_cleaned/clean_working"), full.names = TRUE)

combined_data <- lapply(excel_files, read.csv) %>%
  bind_rows() %>%
  arrange(plotid, time2)

write.csv(combined_data,paste0(mid_dir,"/4a_clean_combined/nordsalt_export.csv"), row.names = F)

