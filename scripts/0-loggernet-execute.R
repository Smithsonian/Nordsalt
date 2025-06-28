#NORDSALT Experiment processing scripts
## Selina Cheng - originated December 5, 2022. Modified for NORDSALT data April 2025 by Liz Westbrook
# This script runs all the functions that were created in "0-loggernet-functions.R"

## ========================== Set up: Load functions ===================================================
# Source functions and packages from "0-merit-processing-functions.R"
source("scripts/0-loggernet-functions.R")

## ========================= Set up: Set paths for data and design documents ===========================
# Set source directory for initial draw of raw data
pool <- "B1"
source_dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/DATA/", pool,"/")

# File path at which you want to save all intermediary output for this script
mid_dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA")

# Get paths for design document, plotname, and varname
design_path <- "design tables/nordsalt_design.csv"
#design_path <- "design tables/MERIT_examples/merit_design_fixed.csv"

plotname_path <- "design tables/nordsalt_plotnames1.csv"

varname_path <- "design tables/nordsalt_design-type.csv"

# ============================ Step 1: Import new files ===============================
# Import new files
#file_import(source_dir, mid_dir) # Not currently working for NORDSALT but not essential

# Remove any empty data tables
data_check(mid_dir)

# ============================ Step 2: Process files ===============================
# Process all files from raw_dir
process(mid_dir)

# ============================ Step 3: Sort files ===============================
# Sort files into individual folders that pair loggers and tables
sorter(mid_dir)

# ============================ Step 4: Normalize files ===============================
# Set up for normalization:
# Edit loggers, tables, and years of data that you want to normalize.
# The "norm_files" function will filter for the files that fulfill these criteria. 
# Note that you can only run tables simultaneously if they collect data at the same time interval.
logger <- c("nordsaltb1", "nordsaltb2", "nordsaltb3")
table <- c("export")
year <- c("2022")

# Get files for normalization
files_for_norm <- norm_files(mid_dir, logger, table, year)

# Set increment for data. 
# Export and MET and TDR use 15 min intervals.
increment <- 60
# NDVI is 60 min intervals. But maybe it's 15 min in 2023?
# increment <- 60

# 2) Run normalization function
norm_nordsalt(files_for_norm, mid_dir, design_path, plotname_path, varname_path, increment)


