#(NORDSALT Experiment) processing scripts
## Selina Cheng - originated December 22, 2022. Adapted for NORDSALT by Liz Westbrook 04/2025

# This script runs functions to clean SD data that were filled with loggernet data by:
# 1) Using range limitation functions to roughly clean the data
# 2) Use rolling SDs to further clean the data
# 3) Create derived variables (deltas) for later use

# This script runs all the functions that were created in "3-cleaning-functions.R"

## ================================= Load functions ============================================
# Source functions and packages from "3-cleaning-functions.R"
source("scripts/2-cleaning-functions.R")

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



