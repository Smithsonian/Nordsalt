library(tidyverse)
library(ggrepel)

nameing_guide <- data.frame(reference =  c("b1","b2","b3","g","u","d","f","s","atemp","ptemp","ltemp"),
                            text = c("Pool 1","Pool 2","Pool 3","Grazed","Ungrazed", "Denmark","Finland","Sweden", "Air Temp","Pin Temp","Lag Temp"))


#load the NORDSALT Data table 
pool <- "b1"
year <- "2023"
variable <- "atemp"
dir <- paste0(Sys.getenv("dropbox_filepath"),"Nordsalt/DATA/data_process/DATA/4c_normal_diffs/")

dt_nordsalt <- read.csv(paste0(dir,"nordsalt_export.csv"))

#get a data table for atemps
dt_nordsalt_var <- dt_nordsalt %>%
  filter(transect == pool, year(time2) == year) %>%
  select(time2, contains(variable), treatment, transect, origin, grazed_ungrazed)

dt_nordsalt_var$time2 <- as.POSIXct(dt_nordsalt_var$time2, format = "%Y-%m-%d %H:%M:%S")

################################make a plot of a temp across different warming levels#####################################################

## make a plot of the warming data for this box 
pool_name <- nameing_guide$text[nameing_guide$reference == pool]

# Graph the temperatures of the selected varable 
graph <- dt_nordsalt_var %>%
  ggplot()+
  geom_line(data = dt_nordsalt_var, aes(x = time2, y = !!sym(paste0(variable, "2")), colour = treatment), na.rm = T)+
  ggtitle(paste0("Values of ", variable, " from the ", year, " experiment in transect ", pool))+
  scale_color_manual(labels = c("w0","w1","w2","w3","w4"), 
                     values = c("#8ABFE5",
                                "#507963",
                                "#8B8000",
                                "#FDC835" ,
                                "#F75077"))+
  theme(legend.title=element_blank()) +  labs(x = "Timestamp", y = paste0("clean ", variable, " values"))+
  theme_light()+
  theme(axis.title.x = element_text(size = 16, face = "bold"),
        axis.title.y = element_text(size=16, face = "bold"))+
  theme(axis.text.x = element_text(size=14, face = "bold"),
        axis.text.y = element_text(size=14, face = "bold"))+
  theme(legend.title=element_blank())


graph


# Graph the temperature differentials  of the selected variable 
graph <- dt_nordsalt_var %>%
  ggplot()+
  geom_line(data = dt_nordsalt_var, aes(x = time2, y = !!sym(paste0("dif_amb_",variable, "_all")), colour = treatment), na.rm = T)+
  ggtitle(paste0("Difference from ambient mean of ", variable, " from the ", year, " experiment in transect ", pool))+
  scale_color_manual(labels = c("w0","w1","w2","w3","w4"), 
                     values = c("#8ABFE5",
                                "#507963",
                                "#8B8000",
                                "#FDC835" ,
                                "#F75077"))+
  theme(legend.title=element_blank()) +  labs(x = "Timestamp", y = paste0("clean ", variable, "_diff values"))+
  theme_light()+
  theme(axis.title.x = element_text(size = 16, face = "bold"),
        axis.title.y = element_text(size=16, face = "bold"))+
  theme(axis.text.x = element_text(size=14, face = "bold"),
        axis.text.y = element_text(size=14, face = "bold"))+
  theme(legend.title=element_blank())



graph
