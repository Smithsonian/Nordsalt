# NORSDALT Experiment processing scripts
## Originated May 2025 by Liz Westbrook for processing MERIT data. 

#This script creates new columns in the NORDSALT data that are calculated differences between transect 
#and full-experiment ambient averages and the temperatures recorded by each treatment.  


#calculate differentials
dt <- read.csv(paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4b_normal_avgs/nordsalt_export_2022.csv"))


dt$dif_amb_atemp_transect <- dt$atemp2 - dt$mean_amb_atemp_transect
dt$dif_amb_atemp_all <- dt$atemp2 - dt$mean_amb_atemp_all
dt$dif_amb_ptemp_transect <- dt$ptemp2 - dt$mean_amb_ptemp_transect
dt$dif_amb_ptemp_all <- dt$ptemp2 - dt$mean_amb_ptemp_all
dt$dif_amb_ltemp_transect <- dt$ltemp2 - dt$mean_amb_ltemp_transect
dt$dif_amb_ltemp_all <- dt$ltemp2 - dt$mean_amb_ltemp_all



write.csv(dt,paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4c_normal_diffs/nordsalt_export_2022_filled.csv"), row.names = F)


#calculate differentials 2023
dt <- read.csv(paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4b_normal_avgs/nordsalt_export_2023.csv"))


dt$dif_amb_atemp_transect <- dt$atemp2 - dt$mean_amb_atemp_transect
dt$dif_amb_atemp_all <- dt$atemp2 - dt$mean_amb_atemp_all
dt$dif_amb_ptemp_transect <- dt$ptemp2 - dt$mean_amb_ptemp_transect
dt$dif_amb_ptemp_all <- dt$ptemp2 - dt$mean_amb_ptemp_all
dt$dif_amb_ltemp_transect <- dt$ltemp2 - dt$mean_amb_ltemp_transect
dt$dif_amb_ltemp_all <- dt$ltemp2 - dt$mean_amb_ltemp_all


write.csv(dt,paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4c_normal_diffs/nordsalt_export_2023_filled.csv"), row.names = F)
