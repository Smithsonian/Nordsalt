#calculate differentials
dt <- read.csv(paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4b_normal_avgs/nordsalt_export.csv"))


dt$dif_amb_atemp_transect <- dt$atemp2 - dt$mean_amb_atemp_transect
dt$dif_amb_atemp_all <- dt$atemp2 - dt$mean_amb_atemp_all
dt$dif_amb_ptemp_transect <- dt$ptemp2 - dt$mean_amb_ptemp_transect
dt$dif_amb_ptemp_all <- dt$ptemp2 - dt$mean_amb_ptemp_all
dt$dif_amb_ltemp_transect <- dt$ltemp2 - dt$mean_amb_ltemp_transect
dt$dif_amb_ltemp_all <- dt$ltemp2 - dt$mean_amb_ltemp_all


write.csv(dt,paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4c_normal_diffs/nordsalt_export.csv"), row.names = F)
