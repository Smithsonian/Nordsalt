#NORDSALT Experiment processing scripts
## Selina Cheng - originated September 8, 2023. Adapted for NORDSALT by Liz Westbrook 04/25

# This scripts stacks SD data to long form for all temp data (pin, lag, deep, surface, above, air)
# So that you can read it into JMP and easily NA data points that are erroneous 

## ============================= Load libraries, set up ===============================================
if (!require("pacman")) install.packages("pacman")
pacman::p_load(reshape2, lubridate, data.table, tools, plyr, tidyverse)

# Set SD dir to get data
data_dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA")

# ============================== Restack data and created derived variables ==========================
# Create path to directories
source_dir <- file.path(data_dir, "4_cleaned/clean_working")
output_dir <- file.path(data_dir, "4a_stacked_for_manual_clean")

year <- "2022"

# Read in files
i <- list.files(source_dir, pattern = year, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

for(n in 1:length(i)){
  # Read in data
  dt <- fread(i[n], tz = "")
  
  pool <- unique(dt$transect)
  
  # Choose only relevant variables
  keep <- c("time2", "logger", "plotid", "treatment","origin","grazed_ungrazed", "transect", "transect_s_treatment",
            "ptemp2", "ltemp2", "atemp2", "m_above2", "satemp2")
  
  dt_temps <- subset(dt, select = keep)
  
  # Stack data so it's in long form
  id_vars <- c("time2", "logger", "plotid", "treatment", "grazed_ungrazed","origin", "transect", "transect_s_treatment")
  
  dt_temps_long <- melt(dt_temps, id.vars = id_vars, measure.vars =, variable.name = "variable_name", na.rm = F)
  
  # Write output path
  output_path <- file.path(output_dir, paste0( pool,"_",year, "_15min_cleaned_long.csv"))
  
  # Write data
  write.table(dt_temps_long, output_path, append = FALSE, quote = FALSE, sep = ",",
              na = "NA", dec = ".", row.names = FALSE,
              col.names = TRUE, qmethod = c("escape", "double"))
}

