# NORSDALT Experiment processing scripts
## Selina Cheng - originated September 1, 2023. Adapted for NORDSALT by Liz Westbrook 04/25

# This script runs functions to clean SD data that were filled with loggernet data by:
# 1) Using range limitation functions to roughly clean the data
# 2) Use rolling SDs to further clean the data
# 3) Create derived variables (deltas) for later use

## ============================= Load libraries ===============================================
if (!require("pacman")) install.packages("pacman")
pacman::p_load(reshape2, lubridate, data.table, tools, plyr, tidyverse)

# ========================== Functions to use while cleaning data =======================================
sd_rm <- function(x){ sd(x, na.rm = TRUE) } # standard deviation with na.rm = TRUE
mean_trim <- function(x){ mean(x, na.rm = TRUE, trim = 0.35) } ## Create mean, trimmed to the middle 30% of values
mean_rm <- function(x){ mean(x, na.rm = TRUE) } # mean function with na.rm = TRUE
max_rm <- function(x){ max(x, na.rm = TRUE) } # mean function with na.rm = TRUE

narrow_range_clean <- function(x){replace(x, x < (0) | x > (30), NA)}
wide_range_clean <- function(x){replace(x, x <= (-10) | x >= (50), NA)}

# =========================== Function 1: clean_data ======================================================
# FUNCTION 1: clean_data(source_dir, output_dir)
# Input: 
# source_dir = directory to pull files from
# output_dir = directory to save files to

# Output: 
# Cleans SD data using range limitation functions and then rolling standard deviations

clean_data <- function(source_dir, output_dir, year){
  # List files from source directory that match year 
  i <- list.files(source_dir, pattern = year, all.files = FALSE,
                  full.names = TRUE, recursive = TRUE,
                  ignore.case = FALSE)
  
  for(n in 1:length(i)){
    # Read in data table
    dt <- fread(i[n], tz = "")
    
    pool <- unique(dt$transect)
    
    # Keep only these columns from loggernet
    keep <- c("time2", "logger", "plotid", "treatment", "grazed_ungrazed", "origin", "transect", "transect_s_treatment",
              "atemp", "ptemp", "ltemp", "pin_b")
    
    dt <- subset(dt, select = keep)
    
    
    if(length(which(!grepl("20..-..-.. ..:..:..", dt$time2))) == 0){
      Sys.setenv(TZ = "Etc/GMT-1") ### set for EST all year long
      dt$time2 <- as.POSIXct(dt$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
    } else {
      print(paste0("These times are formatted incorrectly: ", dt$time2[which(!grepl("20..-..-.. ..:..:..", dt$time2))]))
    }
    
    # Apply range limitation functions
    #dt[, m_above := wide_range_clean(m_above)]
    dt[, atemp := wide_range_clean(atemp)]
    dt[,ltemp := narrow_range_clean(ltemp)]
    dt[,ptemp := narrow_range_clean(ptemp)]
   # dt[,satemp := narrow_range_clean(satemp)]
  
    ##create derived variables for cleaning
    dt[,yday := yday(time2)]
    dt[,hour := hour(time2)]
  
    # Creating key for grouping data
    key2 <-  c("transect","logger","yday","hour") 
    setkeyv(dt, key2)
    
  # Data cleaning functions, developed to be all same for each variable
  # only evaluate hourly and 2day stdev if they are NOT NA
  # If one is NA, the code will evaluate the other
  # If both hourly and 2day stdev are NA, then just set data to NA (this should be very few instances, << 100)
  # PinTemp ----------------------------------------------------------------------
  dt[,ptemp2 := dt$ptemp] #raw data to ghost variable

  dt[,m_ptemp := mean_trim(ptemp), by = key2] #mean of zone by hour for each day
  dt[,m_ptemp_upper := m_ptemp + 5 ,]
  dt[,m_ptemp_lower := m_ptemp - 5 ,]

  dt$ptemp2 <- ifelse(dt$ptemp2 > dt$m_ptemp_upper, NA, dt$ptemp2) #remove more than 3 C away from mean
  dt$ptemp2 <- ifelse(dt$ptemp2 < dt$m_ptemp_lower, NA, dt$ptemp2)
  
  dt[, roll_mean_pintemp := frollapply(ptemp, 4, mean_rm, fill = NA, align = c("center")), by = "plotid"]
  dt$ptemp2 <- ifelse(dt$ptemp2 > (dt$roll_mean_pintemp + 0.5) |
                                 dt$ptemp2 < (dt$roll_mean_pintemp - 0.5),
                               NA, dt$ptemp2)

  # dt[, ptemp_hr := frollapply(ptemp2, 4, sd_rm, fill = NA, align = c("center")), by = "plotid"] #moving window sd for 1 hr
  # # dt[, ptemp_2day := frollapply(ptemp2, 192, sd_rm, fill = NA, align = c("center")), by = "plotid"] # moving window sd for 2 days
  # 
  # # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  # # Change quantile threshold
  # dt$ptemp2 <- ifelse(!is.na(dt$ptemp_hr),
  #                            ifelse(dt$ptemp_hr > quantile(dt$ptemp_hr, 0.995, na.rm = TRUE), NA, dt$ptemp2), dt$ptemp2) #remove data based on sd quantile

  # If 2 day standard deviation is NOT NA, run the quantile test
  # dt$ptemp2 <- ifelse(!is.na(dt$ptemp_2day), 
  #                            ifelse(dt$ptemp_2day > quantile(dt$ptemp_2day, 0.99, na.rm = TRUE), NA, dt$ptemp2), dt$ptemp2)
  
  # If standard deviation of hour and 2 day are both NA, then just NA the data...
  # dt$ptemp2 <- ifelse(is.na(dt$ptemp_hr) & is.na(dt$ptemp_2day), NA, dt$ptemp2)
  
  # # ltemp ----------------------------------------------------------------------
  dt[,ltemp2 := dt$ltemp] #raw data to ghost variable

  dt[,m_ltemp:= mean_trim(ltemp), by = key2] # #mean of zone by hour for each day
  dt[,m_ltemp_upper:= m_ltemp + 5 ,] # median boundary
  dt[,m_ltemp_lower:= m_ltemp - 5 ,]

  dt$ltemp2 <- ifelse(dt$ltemp2 > dt$m_ltemp_upper, NA, dt$ltemp2)#remove more than 5 C away from mean
  dt$ltemp2 <- ifelse(dt$ltemp2 < dt$m_ltemp_lower, NA, dt$ltemp2)
  
  # dt[, m_ltemp_hr := frollapply(ltemp, 2, mean_trim, fill = NA, align = c("center"))]
  # dt$ltemp2 <- ifelse(dt$ltemp2 > (dt$m_ltemp_hr + 0.15) | dt$ltemp2 < (dt$m_ltemp_hr - 0.15), 
  #                                        NA, dt$ltemp2)
  
  # dt[,ltemp_hr:= frollapply(ltemp2, 4, sd_rm, fill = NA, align = c("center")), by = "plotid"] #moving window sd for 1 hr
  # # dt[,ltemp_2day:= frollapply(ltemp2, 192, sd_rm, fill = NA, align = c("center")), by = "plotid"] # moving window sd for 2 days
  # 
  # # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  # dt$ltemp2 <- ifelse(!is.na(dt$ltemp_hr),
  #                            ifelse(dt$ltemp_hr > quantile(dt$ltemp_hr, 0.995, na.rm = TRUE), NA, dt$ltemp2), dt$ltemp2) #remove based on sd quantile

  # # if 2 day standard deviation is NOT NA, and 2day stdev is greater than 95% quantile, remove data
  # dt$ltemp2 <- ifelse(!is.na(dt$ltemp_2day),
  #                            ifelse(dt$ltemp_2day > quantile(dt$ltemp_2day, 0.99, na.rm = TRUE), NA, dt$ltemp2), dt$ltemp2)
  # 
  # # If standard deviation of hour and 2 day are both NA, then just NA the data...
  # dt$ltemp2 <- ifelse(is.na(dt$ltemp_hr) & is.na(dt$ltemp_2day), NA, dt$ltemp2)

  # Surface ----------------------------------------------------------------------
  #dt[,satemp2 := dt$satemp] #raw data to ghost variable

  #dt[,m_satemp:= mean_trim(satemp), by = key2] #mean of zone by hour for each day
  #dt[,m_satemp_upper:= m_satemp + 10 ,] # median boundary
  #dt[,m_satemp_lower:= m_satemp - 10 ,]

  #dt$satemp2 <- ifelse(dt$satemp2 > dt$m_satemp_upper, NA, dt$satemp2)#remove more than 5 C away from mean
  #dt$satemp2 <- ifelse(dt$satemp2 < dt$m_satemp_lower, NA, dt$satemp2)

  # dt[,satemp_hr:= frollapply(satemp2, 2, sd_rm, fill = NA, align = c("center")), by = "plotid"] #moving window sd for 1 hr
  # # # dt[,satemp_2day:= frollapply(satemp2, 192, sd_rm, fill = NA, align = c("center")), by = "plotid"] # moving window sd for 3 days
  # # 
  # # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  # dt$satemp2 <- ifelse(!is.na(dt$satemp_hr),
  #                                ifelse(dt$satemp_hr > quantile(dt$satemp_hr, 0.995, na.rm=TRUE), NA, dt$satemp2),
  #                                dt$satemp2)# remove based on sd quantile
  # # # if 2 day standard deviation is NOT NA, and 2day stdev is greater than 95% quantile, remove data
  # dt$satemp2 <- ifelse(!is.na(dt$satemp_2day),
  #                                ifelse(dt$satemp_2day > quantile(dt$satemp_2day, 0.99, na.rm=TRUE), NA, dt$satemp2),
  #                                dt$satemp2)
  # 
  # # If standard deviation of hour and 2 day are both NA, then just NA the data...
  # dt$satemp2 <- ifelse(is.na(dt$satemp_hr) & is.na(dt$satemp_2day), NA, dt$satemp2)

  # Above ----------------------------------------------------------------------
  #dt[,m_above2 := dt$m_above] #raw data to ghost variable

  #dt[,m_m_above:= mean_trim(m_above), by = key2] #mean of zone by hour for each day
  #dt[,m_m_above_upper:= m_m_above + 10 ,] # median boundary
  #dt[,m_m_above_lower:= m_m_above - 10 ,]

  #dt$m_above2 <- ifelse(dt$m_above2 > dt$m_m_above_upper, NA, dt$m_above2) #remove more than 50% over median
  #dt$m_above2 <- ifelse(dt$m_above2 < dt$m_m_above_lower, NA, dt$m_above2)

  #dt[,m_above_hr:= frollapply(m_above2, 4, sd_rm, fill = NA, align = c("center")), by = "plotid"] #moving window sd for 1 hr
  # dt[,m_above_2day:= frollapply(m_above2, 192, sd_rm, fill = NA, align = c("center")), by = "plotid"] # moving window sd for 3 days

  # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  #dt$m_above2 <- ifelse(!is.na(dt$m_above_hr),
   #                            ifelse(dt$m_above_hr > quantile(dt$m_above_hr, 0.995, na.rm=TRUE), NA, dt$m_above2),
    #                           dt$m_above2) #remove based on sd quantile
# 
#   # # if 2 day standard deviation is NOT NA, and 2day stdev is greater than 95% quantile, remove data
#   dt$m_above2 <- ifelse(!is.na(dt$m_above_2day),
#                                ifelse(dt$m_above_2day > quantile(dt$m_above_2day, 0.99, na.rm=TRUE), NA, dt$m_above2),
#                                dt$m_above2)
# 
#   # If standard deviation of hour and 2 day are both NA, then just NA the data...
#   dt$m_above2 <- ifelse(is.na(dt$m_above_hr) & is.na(dt$m_above_2day), NA, dt$m_above2)

  # Air ----------------------------------------------------------------------
  dt[,atemp2 := dt$atemp] #raw data to ghost variable

  dt[,m_atemp:= mean_trim(atemp), by = key2] # median line by site
  dt[,m_atemp_upper:= m_atemp + 10 ,] # median boundary
  dt[,m_atemp_lower:= m_atemp - 10 ,]

  dt$atemp2 <- ifelse(dt$atemp2 > dt$m_atemp_upper, NA, dt$atemp2) #remove more than 50% over median
  dt$atemp2 <- ifelse(dt$atemp2 < dt$m_atemp_lower, NA, dt$atemp2)

  dt[,atemp_hr:= frollapply(atemp2, 4, sd_rm, fill = NA, align = c("center")), by = "plotid"] #moving window sd for 1 hr
  # dt[,atemp_2day:= frollapply(atemp2, 192, sd_rm, fill = NA, align = c("center")), by = "plotid"] # moving window sd for 3 days

  # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  dt$atemp2 <- ifelse(!is.na(dt$atemp_hr),
                             ifelse(dt$atemp_hr > quantile(dt$atemp_hr, 0.995, na.rm=TRUE), NA, dt$atemp2),
                             dt$atemp2)#remove based on sd quantile
  # # # if 2 day standard deviation is NOT NA, and 2day stdev is greater than 95% quantile, remove data
  # dt$atemp2 <- ifelse(!is.na(dt$atemp_2day),
  #                            ifelse(dt$atemp_2day > quantile(dt$atemp_2day, 0.99, na.rm=TRUE), NA, dt$atemp2),
  #                            dt$atemp2)
  # 
  # # If standard deviation of hour and 2 day are both NA, then just NA the data...
  # dt$atemp2 <- ifelse(is.na(dt$atemp_hr) & is.na(dt$atemp_2day), NA, dt$atemp2)
  
  
  
  keep <- c("time2", "logger", "plotid", "treatment", "grazed_ungrazed", "origin", "transect", "transect_s_treatment",
                 "atemp", "ptemp", "ltemp", "pin_b", "atemp2","ptemp2","ltemp2")
  
  dt <- subset(dt, select = keep)
  

  # -------------------------------- Write data ----------------------------------------------
  # Create file name
  file_name <- paste0(pool,"_",year(dt$time2[1]), "_cleaned.csv")
  out_path <- file.path(output_dir, file_name)
  
  dt$time2 <- format(as.POSIXct(dt$time2, tz="Etc/GMT-1"), format="%Y-%m-%d %H:%M:%S")

  # Save table
  write.table(dt, out_path, append = FALSE, quote = FALSE, sep = ",",
              na = "NA", dec = ".", row.names = FALSE,
              col.names = TRUE, qmethod = c("escape", "double"))

  print("Done with 1 file")
  }
}
