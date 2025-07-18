library(tidyverse)
library(ggrepel)

nameing_guide <- data.frame(reference =  c("b1","b2","b3","g","u","d","f","s","atemp","ptemp","ltemp"),
                            text = c("Pool 1","Pool 2","Pool 3","Grazed","Ungrazed", "Denmark","Finland","Sweden", "Air Temp","Pin Temp","Lag Temp"))


#load the NORDSALT Data table 
pool <- "b1"
year <- "2022"
dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/5_derived/")

dt_nordsalt <- read.csv(paste0(dir,pool,"_",year,"_15min_cleaned_derived.csv"))

#get a data table for atemps
dt_nordsalt_air <- dt_nordsalt %>%
  select(time2, contains("atemp"))

#get a data table for ptemps
dt_nordsalt2 <- dt_nordsalt %>%
  select(time2, airtemp_clean, pintemp_clean, lagtemp_clean, treatment, origin, grazed_ungrazed, z_pin_delta_all, z_lag_delta_all)

###################################remake the plots shown in the manuscript #########################################

################################make a plot of a temp across different warming levels#####################################################

## make a plot of the warming data for this box 
pool_name <- nameing_guide$text[nameing_guide$reference == pool]

# Graph
graph <- dt_nordsalt2 %>%
  ggplot()+
  geom_line(data = dt, aes(x = TIMESTAMP, y = as.numeric(temperature_c), colour = warming_level), na.rm = T)+
  geom_line(data = target_ref, aes(x = TIMESTAMP, y = ptemp, colour = treatment), na.rm = T,linetype = "dashed", alpha = 0.75)+
  ggtitle(paste0("Test"))+
  scale_color_manual(labels = c("w0","w1","w2","w3","w4"), 
                     values = c("#8ABFE5",
                                "#507963",
                                "#8B8000",
                                "#FDC835" ,
                                "#F75077"))+
  theme(legend.title=element_blank()) +  labs(x = "Timestamp", y = expression("Soil Temperature " (degree~C)))+
  theme_light()+
  theme(axis.title.x = element_text(size = 16, face = "bold"),
        axis.title.y = element_text(size=16, face = "bold"))+
  theme(axis.text.x = element_text(size=14, face = "bold"),
        axis.text.y = element_text(size=14, face = "bold"))+
  theme(legend.title=element_blank())

ggsave(paste0(Sys.getenv("repository_filepath"),"Nordsalt/figures/Warming_Samples/",var,"_",g_ug,"_",origin_of_plant,"_",pool,".png"), plot = graph)



