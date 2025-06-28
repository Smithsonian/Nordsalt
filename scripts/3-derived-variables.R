# Wadden Sea (MERIT Experiment) processing scripts
## Selina Cheng - originated September 8, 2023.

# This scripts unstacks (converts to wide) the SD data that have been algorithmically and manually cleaned (in JMP).
# 1) Converts to wide and joins back with original SD data
# 2) Creates derived variables (deltas) for later use

## ============================= Load libraries, set up ===============================================
if (!require("pacman")) install.packages("pacman")
pacman::p_load(reshape2, lubridate, data.table, tools, plyr, tidyverse)

mean_rm <- function(x){ mean(x, na.rm = TRUE) } # mean function with na.rm = TRUE

# Set SD dir to get data
data_dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/")

# ============================== Restack data and created derived variables ==========================
# Create path to directories
source_dir_og <- file.path(data_dir, "4_cleaned/clean_working")
source_dir <- file.path(data_dir, "4b_manually_cleaned")
output_dir <- file.path(data_dir, "5_derived")

# Output: 
# Creates derived temperature delta variables in data 
# Read in temperature data that was manually cleaned
pool <- "b3"
b <- list.files(source_dir, pattern = pool, all.files = FALSE,
 full.names = TRUE, recursive = F,
 ignore.case = FALSE)
  
# Read in original SD data
a <- list.files(source_dir_og, pattern = pool, all.files = FALSE,
 full.names = TRUE, recursive = F,
 ignore.case = FALSE)
  
  for(n in 1:length(b)){
    # Read in data table
    dt <- fread(b[n], tz = "")
    
    dt <- unique(dt)
  
    # Cast data to wide form
    dt_wide <- dcast(dt, time2+logger+plotid+treatment+grazed_ungrazed+origin+transect+transect_s_treatment ~ variable_name, 
                     fun.aggregate = function(x) mean(x, na.rm = TRUE), subset = NULL, drop = TRUE, value.var = "value")
    
    # Format timestamp
    dt_wide$time2 <- as.POSIXct(dt_wide$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
    
    # Get year variable
    year <- substr(basename(b[n]),4,7)
    
    # Read in original SD data
    dt_sd <- fread(a[grepl(year, a)], tz = "")
    
    # Reformat dt_sd time var
    dt_sd$time2 <- as.POSIXct(dt_sd$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
    
    # Join original SD data with new variables
    setDT(dt_sd)
    setDT(dt_wide)
    
    # Change names of manually cleaned data for join
    setnames(dt_wide, c("m_above2", "atemp2", "ltemp2", "ptemp2", "satemp2"), 
             c("mabove_clean", "airtemp_clean", "lagtemp_clean", "pintemp_clean", "surfacetemp_clean"))
    
    # Set merge keys
    mergekey <- c("time2", "logger","plotid", "treatment", "origin","grazed_ungrazed", "transect", "transect_s_treatment")
    setkeyv(dt_sd, mergekey)
    setkeyv(dt_wide, mergekey)
    
    dt_clean <- merge(dt_sd, dt_wide, by = mergekey)
    
    # Keep only these variables
    # Drops 
    # If using SD card data:
    #keep <- c("time2", "logger", "id", "plotid", "treatment", "zone",  "transect", "site_x_treatment", "pinTemp_sd",
     #         "lagTemp_sd", "surfaceTemp_sd", "deepTemp_sd", "aboveTemp_sd", "airTemp_sd", "PTavg_5_sd", "LTavg_5_sd",
      #        "STavg_5_sd", "DTavg_5_sd", "ATavg_5_sd", "AiTavg_5_sd", "PTsd_5", "LTsd_5", "STsd_5", "DTsd_5", "ATsd_5",
       #       "AiTsd_5", "dutySurface_sd", "dutyDeep_sd", "Arduino_Code_sd", "CS_Code_sd",
        #      "above_filled", "air_filled", "deep_filled", "lag_filled", "pin_filled", "surface_filled",
         #     "abovetemp_clean", "airtemp_clean", "deeptemp_clean", "lagtemp_clean", "pintemp_clean", "surfacetemp_clean")
    
    # If using only loggernet data:
     keep <- c("time2", "logger", "plotid", "treatment", "origin","grazed_ungrazed",  "transect", "transect_s_treatment", 
               "m_above", "atemp", "ltemp", "ptemp", "satemp",
               "mabove_clean", "airtemp_clean", "lagtemp_clean", "pintemp_clean", "surfacetemp_clean")
              
    dt_clean2 <- subset(dt_clean, select = keep)
    
    # ----------------------------- Create derived variables --------------------------------------
    
  #########All ambients################
    # Create ambient deltas for all ambients
    group <- c("time2", "treatment")
  
    # Create spatial means across treatment and site. So this should be the same for all PIO amb, for example
    dt_clean2[, z_trt_mean_pin_all:= mean_rm(pintemp_clean), by = group]
    dt_clean2[, z_trt_mean_lag_all:= mean_rm(lagtemp_clean), by = group]
    dt_clean2[, z_trt_mean_surface_all:= mean_rm(surfacetemp_clean), by = group]
    dt_clean2[, z_trt_mean_above_all:= mean_rm(mabove_clean), by = group]
    dt_clean2[, z_trt_mean_air_all:= mean_rm(airtemp_clean), by = group]
    
    # Create ambients
    # Isolate just ambient data and assign new ambient names
    dt_amb_all <- dt_clean2[treatment =="w0", .(time2, z_trt_mean_pin_all, z_trt_mean_lag_all, z_trt_mean_surface_all, z_trt_mean_above_all, z_trt_mean_air_all)]
    newnames <- c("time2","z_trt_amb_pin_all", "z_trt_amb_lag_all", "z_trt_amb_surface_all", "z_trt_amb_above_all","z_trt_amb_air_all")
    setnames(dt_amb_all, names(dt_amb_all), newnames)
    dt_amb_all <- unique(dt_amb_all)
    
    # Merged ambients back with original data
    setkey(dt_amb_all, "time2")
    setkey(dt_clean2, "time2") 
    
    dt3 <- merge(dt_amb_all, dt_clean2, all = TRUE, allow.cartesian = TRUE, by = c("time2"))
    
    
    ##########grazed/ungrazed amients#############
    # Create ambient deltas for all ambients
    group <- c("time2", "treatment","grazed_ungrazed")
    
    # Create spatial means across treatment and site. So this should be the same for all PIO amb, for example
    dt3[, z_trt_mean_pin_g_ug:= mean_rm(pintemp_clean), by = group]
    dt3[, z_trt_mean_lag_g_ug:= mean_rm(lagtemp_clean), by = group]
    dt3[, z_trt_mean_surface_g_ug:= mean_rm(surfacetemp_clean), by = group]
    dt3[, z_trt_mean_above_g_ug:= mean_rm(mabove_clean), by = group]
    dt3[, z_trt_mean_air_g_ug:= mean_rm(airtemp_clean), by = group]
    
    
    # Isolate just ambient data and assign new ambient names
    dt_amb_g_ug <- dt3[treatment =="w0", .(time2, z_trt_mean_pin_g_ug, z_trt_mean_lag_g_ug, z_trt_mean_surface_g_ug, z_trt_mean_above_g_ug, z_trt_mean_air_g_ug)]
    newnames <- c("time2","z_trt_amb_pin_g_ug", "z_trt_amb_lag_g_ug", "z_trt_amb_surface_g_ug", "z_trt_amb_above_g_ug","z_trt_amb_air_g_ug")
    setnames(dt_amb_g_ug, names(dt_amb_g_ug), newnames)
    dt_amb_g_ug <- unique(dt_amb_g_ug)
    
    
    # Merged ambients back with original data
    setkey(dt_amb_g_ug, "time2")
    setkey(dt3, "time2") 
    
    dt3 <- merge(dt_amb_g_ug, dt3 , all = TRUE, allow.cartesian = TRUE, by = c("time2"))
    
    
    ##########origin based ambients#############
    # Create ambient deltas for all ambients
    group <- c("time2", "treatment","origin")
    
    # Create spatial means across treatment and site. So this should be the same for all PIO amb, for example
    dt3[, z_trt_mean_pin_origin:= mean_rm(pintemp_clean), by = group]
    dt3[, z_trt_mean_lag_origin:= mean_rm(lagtemp_clean), by = group]
    dt3[, z_trt_mean_surface_origin:= mean_rm(surfacetemp_clean), by = group]
    dt3[, z_trt_mean_above_origin:= mean_rm(mabove_clean), by = group]
    dt3[, z_trt_mean_air_origin:= mean_rm(airtemp_clean), by = group]
    
    
    # Isolate just ambient data and assign new ambient names
    dt_amb_origin <- dt3[treatment =="w0", .(time2, z_trt_mean_pin_origin, z_trt_mean_lag_origin, z_trt_mean_surface_origin, z_trt_mean_above_origin, z_trt_mean_air_origin)]
    newnames <- c("time2","z_trt_amb_pin_origin", "z_trt_amb_lag_origin", "z_trt_amb_surface_origin", "z_trt_amb_above_origin","z_trt_amb_air_origin")
    setnames(dt_amb_origin, names(dt_amb_origin), newnames)
    dt_amb_origin <- unique(dt_amb_origin)
    
    
    # Merged ambients back with original data
    setkey(dt_amb_origin, "time2")
    setkey(dt3, "time2") 
    
    dt3 <- merge(dt_amb_origin, dt3, all = TRUE, allow.cartesian = TRUE, by = c("time2"))
    
    ###################All ambients#############################
    #create plot and zone deltas
    # Delta between zone treatment mean and zone ambient mean
    # Delta between individual treatment and individual ambient
    dt3[, z_pin_delta_all:= z_trt_mean_pin_all - z_trt_amb_pin_all,]
    dt3[, pin_delta_all:= pintemp_clean - z_trt_amb_pin_all,]
    
    dt3[, z_lag_delta_all:= z_trt_mean_lag_all - z_trt_amb_lag_all,]
    dt3[, lag_delta_all:= lagtemp_clean - z_trt_amb_lag_all,]
    
    dt3[, z_surface_delta_all:= z_trt_mean_surface_all - z_trt_amb_surface_all,]
    dt3[, surface_delta_all:= surfacetemp_clean - z_trt_amb_surface_all,]
    
    
    dt3[, z_above_delta_all:= z_trt_mean_above_all - z_trt_amb_above_all,]
    dt3[, above_delta_all:= mabove_clean - z_trt_amb_above_all,]
    
    dt3[, z_air_delta_all:= z_trt_mean_air_all - z_trt_amb_air_all,]
    dt3[, air_delta_all:= airtemp_clean - z_trt_amb_air_all,]
    
    
    ###################g_ug based ambients#############################
    #create plot and zone deltas
    # Delta between zone treatment mean and zone ambient mean
    # Delta between individual treatment and individual ambient
    dt3[, z_pin_delta_g_ug:= z_trt_mean_pin_g_ug - z_trt_amb_pin_g_ug,]
    dt3[, pin_delta_g_ug:= pintemp_clean - z_trt_amb_pin_g_ug,]
    
    dt3[, z_lag_delta_g_ug:= z_trt_mean_lag_g_ug - z_trt_amb_lag_g_ug,]
    dt3[, lag_delta_g_ug:= lagtemp_clean - z_trt_amb_lag_g_ug,]
    
    dt3[, z_surface_delta_g_ug:= z_trt_mean_surface_g_ug - z_trt_amb_surface_g_ug,]
    dt3[, surface_delta_g_ug:= surfacetemp_clean - z_trt_amb_surface_g_ug,]
    
    
    dt3[, z_above_delta_g_ug:= z_trt_mean_above_g_ug - z_trt_amb_above_g_ug,]
    dt3[, above_delta_g_ug:= mabove_clean - z_trt_amb_above_g_ug,]
    
    dt3[, z_air_delta_g_ug:= z_trt_mean_air_g_ug - z_trt_amb_air_g_ug,]
    dt3[, air_delta_g_ug:= airtemp_clean - z_trt_amb_air_g_ug,]
    
    ###################origin based ambients#############################
    #create plot and zone deltas
    # Delta between zone treatment mean and zone ambient mean
    # Delta between individual treatment and individual ambient
    dt3[, z_pin_delta_origin:= z_trt_mean_pin_origin - z_trt_amb_pin_origin,]
    dt3[, pin_delta_origin:= pintemp_clean - z_trt_amb_pin_origin,]
    
    dt3[, z_lag_delta_origin:= z_trt_mean_lag_origin - z_trt_amb_lag_origin,]
    dt3[, lag_delta_origin:= lagtemp_clean - z_trt_amb_lag_origin,]
    
    dt3[, z_surface_delta_origin:= z_trt_mean_surface_origin - z_trt_amb_surface_origin,]
    dt3[, surface_delta_origin:= surfacetemp_clean - z_trt_amb_surface_origin,]
    
    
    dt3[, z_above_delta_origin:= z_trt_mean_above_origin - z_trt_amb_above_origin,]
    dt3[, above_delta_origin:= mabove_clean - z_trt_amb_above_origin,]
    
    dt3[, z_air_delta_origin:= z_trt_mean_air_origin - z_trt_amb_air_origin,]
    dt3[, air_delta_origin:= airtemp_clean - z_trt_amb_air_origin,]
    
    # --------------------------------- Write data ---------------------------------------------
    # Create file name
    file_name <- paste0(pool,"_", year, "_60min_cleaned_derived.csv")
    out_path <- file.path(output_dir, file_name)
    
    dt3$time2 <- format(as.POSIXct(dt3$time2, tz="Etc/GMT-1"), format="%Y-%m-%d %H:%M:%S")
    
    # Save table
    write.table(dt3, out_path, append = FALSE, quote = FALSE, sep = ",",
                na = "NA", dec = ".", row.names = FALSE,
                col.names = TRUE, qmethod = c("escape", "double"))
    
    print("Done with 1 file")
  }
  