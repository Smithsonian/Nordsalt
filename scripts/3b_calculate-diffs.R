#calculate differentials
dt <- read.csv(paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/3b_normal_avgs/nordsalt_export.csv"))


dt$dif_amb_atemp_transect <- dt$atemp - dt$mean_amb_atemp_transect
dt$dif_amb_atemp_all <- dt$atemp - dt$mean_amb_atemp_all
dt$dif_amb_ptemp_transect <- dt$atemp - dt$mean_amb_ptemp_transect
dt$dif_amb_ptemp_all <- dt$atemp - dt$mean_amb_ptemp_all
dt$dif_amb_ltemp_transect <- dt$atemp - dt$mean_amb_ltemp_transect
dt$dif_amb_ltemp_all <- dt$atemp - dt$mean_amb_ltemp_all


write.csv(dt,paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/3c_normal_diffs/nordsalt_export.csv"), row.names = F)
