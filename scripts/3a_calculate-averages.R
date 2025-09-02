
# NORSDALT Experiment processing scripts
## Originated May 2025 by Liz Westbrook for processing MERIT data. 

library(tidyverse)
library(data.table)
library(lubridate)

mean_rm <- function(x){ mean(x, na.rm = TRUE) } # mean function with na.rm = TRUE

# Load data
dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4ab_manually_cleaned/")
file <- list.files(dir, full.names = TRUE)
dt <- read.csv(file)

# Ensure time2 is POSIXct for filtering
dt$time2 <- as.POSIXct(dt$time2, format = "%Y/%m/%d %I:%M:%S %p")

# Split into 2022 and 2023 datasets
dt_2022 <- dt %>% filter(format(time2, "%Y") == "2022") %>%
  distinct(plotid, time2, .keep_all = TRUE)

dt_2022 <- dt_2022 %>% 
  arrange(time2)

dt_2023 <- dt %>% filter(format(time2, "%Y") == "2023") %>%
  distinct(plotid, time2, .keep_all = TRUE)

dt_2023 <- dt_2023 %>% 
  arrange(time2)


## Remove the bad time period from B2 W3 ug fromt he start of June 2023 to the start of october 2023 
dt_2023 <- dt_2023 %>%
  mutate(
    atemp2 = if_else(
      treatment == "w3" &
        transect == "b2" &
        grazed_ungrazed == "ug" &
        time2 >= as.Date("2023-06-01") &
        time2 < as.Date("2023-10-01"),
      NA,  
      atemp2     
    )
  )



### Fixing the B2 W3 UG issue with filling 
# create an average of B1 and B3 atemp 2 values 
dt_b1_w3_ug <- dt_2023 %>% 
  filter(transect == 'b1', treatment == 'w3', grazed_ungrazed == 'ug') %>% 
  select (time2, atemp2)

dt_b1_w3_ug <- dt_b1_w3_ug[!duplicated(dt_b1_w3_ug[, c("time2")]), ]

dt_b3_w3_ug <- dt_2023 %>% 
  filter(transect == 'b3', treatment == 'w3', grazed_ungrazed == 'ug') %>% 
  select (time2, atemp2)
  
dt_b3_w3_ug <- dt_b3_w3_ug[!duplicated(dt_b3_w3_ug[, c("time2")]), ]

dt_avg <- inner_join(dt_b1_w3_ug, dt_b3_w3_ug, by = "time2", suffix = c("_b1", "_b3")) %>%
  mutate(atemp_avg = rowMeans(select(., atemp2_b1, atemp2_b3), na.rm = TRUE)) %>%
  select(time2, atemp_avg)

###find where B2 has been cleaned with the manual cleaning
dt_b2_missing <- dt_2023 %>%
  filter(transect == 'b2', treatment == 'w3', grazed_ungrazed == 'ug', is.na(atemp2)) %>%
  select(plotid, time2, everything()) 

dt_b2_missing <- dt_b2_missing[!duplicated(dt_b2_missing[, c("time2", "transect_s_treatment")]), ]

#Fill with the averages from B1 and B3 
dt_b2_filled <- dt_b2_missing %>%
  left_join(dt_avg, by = "time2") %>%
  mutate(atemp2 = if_else(is.na(atemp2), atemp_avg, atemp2)) %>%
  select(-atemp_avg)  

#rejoind with the original data set 
dt_2023<- dt_2023 %>%
  rows_update(dt_b2_filled, by = c("plotid","time2"))

# Function to process each year's data
process_nordsalt <- function(data) {
  ambient_dt <- data %>% filter(treatment == "w0")
  setDT(ambient_dt)
  
  group <- c("time2", "origin", "transect")
  group2 <- c("time2", "origin")
  
  # Create ambient means
  ambient_dt[, mean_amb_atemp_transect := mean_rm(atemp2), by = group]
  ambient_dt[, mean_amb_atemp_all := mean_rm(atemp2), by = group2]
  ambient_dt[, mean_amb_ptemp_transect := mean_rm(ptemp2), by = group]
  ambient_dt[, mean_amb_ptemp_all := mean_rm(ptemp2), by = group2]
  ambient_dt[, mean_amb_ltemp_transect := mean_rm(ltemp2), by = group]
  ambient_dt[, mean_amb_ltemp_all := mean_rm(ltemp2), by = group2]
  
  # Replace missing values
  ambient_dt$atemp2[is.na(ambient_dt$atemp2)] <- ambient_dt$mean_amb_atemp_all[is.na(ambient_dt$atemp2)]
  ambient_dt$ptemp2[is.na(ambient_dt$ptemp2)] <- ambient_dt$mean_amb_ptemp_all[is.na(ambient_dt$ptemp2)]
  ambient_dt$ltemp2[is.na(ambient_dt$ltemp2)] <- ambient_dt$mean_amb_ltemp_all[is.na(ambient_dt$ltemp2)]
  
  # Merge ambient averages back into full dataset
  data_new <- merge(data, ambient_dt[, .(time2, origin, transect,
                                         mean_amb_atemp_transect,
                                         mean_amb_ptemp_transect,
                                         mean_amb_ltemp_transect)],
                    by = c("time2", "origin", "transect"), all.x = TRUE)
  
  data_new <- merge(data_new, ambient_dt[, .(time2, origin,
                                             mean_amb_atemp_all,
                                             mean_amb_ptemp_all,
                                             mean_amb_ltemp_all)],
                    by = c("time2", "origin"), all.x = TRUE)
  
  data_new <- unique(data_new) %>% arrange(plotid, time2)
  
  data_new$time2 <- as.POSIXct(data_new$time2, format = "%Y-%m-%d %H:%M:%S")
  data_new$time2 <- format(data_new$time2, "%Y-%m-%d %H:%M:%S")
  return(data_new)
}

# Process each year separately
dt_2022_processed <- process_nordsalt(dt_2022)
dt_2023_processed <- process_nordsalt(dt_2023)

# Save to CSV
write.csv(dt_2022_processed, paste0(Sys.getenv("dropbox_filepath"),
                                    "Nordsalt/DATA/data_process/DATA/4b_normal_avgs/nordsalt_export_2022.csv"),
          row.names = FALSE)

write.csv(dt_2023_processed, paste0(Sys.getenv("dropbox_filepath"),
                                    "Nordsalt/DATA/data_process/DATA/4b_normal_avgs/nordsalt_export_2023.csv"),
          row.names = FALSE)
