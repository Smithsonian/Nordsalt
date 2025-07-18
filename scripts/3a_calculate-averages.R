## add the ambient averages columns 
library(tidyverse)
library(data.table)

mean_rm <- function(x){ mean(x, na.rm = TRUE) } # mean function with na.rm = TRUE

dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4a_clean_combined/")

file <- list.files(dir, full.names = T)

dt <- read.csv(file)

ambient_dt <- dt %>% 
  filter(treatment == "w0")

setDT(ambient_dt)

# Create ambient deltas for all ambients
group <- c("time2", "origin", "transect")
group2 <- c("time2","origin")

# create the ambient means. 
ambient_dt[,mean_amb_atemp_transect:= mean_rm(atemp2), by = group]
ambient_dt[,mean_amb_atemp_all:= mean_rm(atemp2), by = group2]
ambient_dt[,mean_amb_ptemp_transect:= mean_rm(ptemp2), by = group]
ambient_dt[,mean_amb_ptemp_all:= mean_rm(ptemp2), by = group2]
ambient_dt[,mean_amb_ltemp_transect:= mean_rm(ltemp2), by = group]
ambient_dt[,mean_amb_ltemp_all:= mean_rm(ltemp2), by = group2]



##test that the data set got set up correctly
test_dt <- ambient_dt %>%
  filter(time2 == "2023-03-19 14:00:00")


## replace missing values with the ambient averages column 
ambient_dt$atemp2[is.na(ambient_dt$atemp2)] <- ambient_dt$mean_amb_atemp_all[is.na(ambient_dt$atemp2)]
ambient_dt$ptemp2[is.na(ambient_dt$ptemp2)] <- ambient_dt$mean_amb_ptemp_all[is.na(ambient_dt$ptemp2)]
ambient_dt$ltemp2[is.na(ambient_dt$ltemp2)] <- ambient_dt$mean_amb_ltemp_all[is.na(ambient_dt$ltemp2)]


## Merge the ambient averages back into the larger data set. 

dt_new <- merge(dt, ambient_dt[, .(time2, origin, transect,
                               mean_amb_atemp_transect,
                               mean_amb_ptemp_transect,
                               mean_amb_ltemp_transect)],
            by = c("time2", "origin", "transect"), all.x = TRUE)

dt_new <- merge(dt_new, ambient_dt[, .(time2, origin,
                               mean_amb_atemp_all,
                               mean_amb_ptemp_all,
                               mean_amb_ltemp_all)],
            by = c("time2", "origin"), all.x = TRUE)


dt_new <- unique(dt_new) %>%
  arrange(plotid, time2)


##test that the data set got set up correctly
test_dt <- dt_new %>%
  filter(time2 == "2023-03-19 14:00:00")


write.csv(dt_new,paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4b_normal_avgs/nordsalt_export.csv"), row.names = F)



