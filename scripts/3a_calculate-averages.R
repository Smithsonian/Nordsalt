## add the ambient averages columns 
library(tidyverse)
library(data.table)

mean_rm <- function(x){ mean(x, na.rm = TRUE) } # mean function with na.rm = TRUE

dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/3a_normal_combined/")

file <- list.files(dir, full.names = T)

dt <- read.csv(file)

ambient_dt <- dt %>% 
  filter(treatment == "w0")

setDT(ambient_dt)

# Create ambient deltas for all ambients
group <- c("time2", "origin", "transect")
group2 <- c("time2","origin")

# create the ambient means. 
ambient_dt[,mean_amb_atemp_transect:= mean_rm(atemp), by = group]
ambient_dt[,mean_amb_atemp_all:= mean_rm(atemp), by = group2]
ambient_dt[,mean_amb_ptemp_transect:= mean_rm(ptemp), by = group]
ambient_dt[,mean_amb_ptemp_all:= mean_rm(ptemp), by = group2]
ambient_dt[,mean_amb_ltemp_transect:= mean_rm(ltemp), by = group]
ambient_dt[,mean_amb_ltemp_all:= mean_rm(ltemp), by = group2]



##test that the data set got set up correctly
test_dt <- ambient_dt %>%
  filter(time2 == "2023-03-19 14:00:00")


# remove any data that is not in an  acceptable range
atemp_upper <- 40
atemp_lower <- -4
ptemp_upper <- 40
ptemp_lower <- -4
ltemp_upper <- 40
ltemp_lower <- -4


ambient_dt$atemp[ambient_dt$atemp < atemp_lower] <- NA
ambient_dt$atemp[ambient_dt$atemp > atemp_upper] <- NA

ambient_dt$ptemp[ambient_dt$ptemp < ptemp_lower] <- NA
ambient_dt$ptemp[ambient_dt$ptemp > ptemp_upper] <- NA

ambient_dt$ltemp[ambient_dt$ltemp < ltemp_lower] <- NA
ambient_dt$ltemp[ambient_dt$ltemp > ltemp_upper] <- NA

## replace missing values with the ambient averages column 
ambient_dt$atemp[is.na(ambient_dt$atemp)] <- ambient_dt$mean_amb_atemp_all[is.na(ambient_dt$atemp)]
ambient_dt$ptemp[is.na(ambient_dt$ptemp)] <- ambient_dt$mean_amb_ptemp_all[is.na(ambient_dt$ptemp)]
ambient_dt$ltemp[is.na(ambient_dt$ltemp)] <- ambient_dt$mean_amb_ltemp_all[is.na(ambient_dt$ltemp)]


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


dt_new <- unique(dt_new)


dt_new <- arrange(dt_new, X)

##test that the data set got set up correctly
test_dt <- dt_new %>%
  filter(time2 == "2023-03-19 14:00:00")


#remove values that are out of range for the non-ambients 
dt_new$atemp[dt_new$atemp < atemp_lower] <- NA
dt_new$atemp[dt_new$atemp > atemp_upper] <- NA

dt_new$ptemp[dt_new$ptemp < ptemp_lower] <- NA
dt_new$ptemp[dt_new$ptemp > ptemp_upper] <- NA

dt_new$ltemp[dt_new$ltemp < ltemp_lower] <- NA
dt_new$ltemp[dt_new$ltemp > ltemp_upper] <- NA


write.csv(dt_new,paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/3b_normal_avgs/nordsalt_export.csv"), row.names = F)



