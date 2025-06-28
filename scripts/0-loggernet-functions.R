# MERIT data processing

# Generally:
# 1. bulk process the loggernet, run process function to turn .DAT files into .CSV files by zone 
# 2. create concatenated no-duplicate, re-normalized versions for that data
# 3. join data tables from various sources (sd, elevation, met, loggernet)

## ================================= Load libraries =======================================================
# Automatically installs packages if not installed and loads them appropriately.
if (!require("pacman")) install.packages("pacman")
pacman::p_load(reshape2, lubridate, data.table, tools, tidyverse, plyr)

## ============================ Function 1: file_import =================================================
# FUNCTION 1: file_import(source_dir, mid_dir)
# Input: 
# source_dir = directory to pull files from
# mid_dir = intermediate directory that you're working in 

# Output: 
# Copies all files from source directory into raw_working directory

file_import <- function(source_dir, mid_dir){
  # Set raw working directory
  raw_working <- file.path(mid_dir, "0_raw", "raw_working")
  # Set raw archive directory
  raw_archive <- file.path(mid_dir, "0_raw", "raw_archive")
  
  # List all files from the source directory folder
  i <- list.files(source_dir, pattern = NULL, all.files = FALSE,
                      full.names = TRUE, recursive = FALSE,
                      ignore.case = FALSE)
  
  # Keep only files that are .dat or .backup extensions
  i <- i[file_ext(i) == "dat" | file_ext(i) == "backup"]
  
  # Compare list of incoming files with files from raw_archive
  # Get base file name (not including all the stuff before it)
  files_base <- basename(i)

  # Read in data from raw_archive
  done_files <- list.files(raw_archive, pattern = NULL, all.files = FALSE,
                           full.names = FALSE, recursive = FALSE,
                           ignore.case = FALSE)
  
  # Only keep files that are NOT in the raw_archive
  keep_files <- i[files_base %in% done_files == F]
  
  # Copy files to raw_working
  file.copy(keep_files, raw_working)
  
  # Print a nice little message
  print(paste0(length(keep_files), " files have been imported."))
}

## ================================ Function 2: datacheck =============================================
# FUNCTION 2: datacheck(mid_dir)
# Input: 
# mid_dir = intermediate directory that you're working in

# Output: 
# Removes any empty data tables

data_check <- function(mid_dir) {
  # Create raw dir file path
  raw_dir <- file.path(mid_dir, "0_raw", "raw_working")
  
  # List files in raw directory
  i <- list.files(raw_dir, pattern = NULL, all.files = FALSE, 
                full.names = TRUE, recursive = TRUE, 
                ignore.case = FALSE) 
  
  for (n in 1:length(i)) {
    # Read in all the files from the list
    dat <- read.table(i[n], sep = ",", quote = "\"'", 
                     dec = ".", na.strings = "NA", colClasses = "character", nrows = 6, 
                     skip = 0, check.names = FALSE,fill=TRUE, 
                     strip.white = FALSE,blank.lines.skip = TRUE) 
    
    # Get the table type from cell 1, 1 in the table
    type <- dat[1,1]	
    
    # Get number of rows in the table
    rows <- nrow(dat) 
    
    # If the type is TOACI1 and the file is < 3 rows, remove
    # If the type is TOA5 and the file is < 5 rows, remove
    if(rows < 3 & type == "TOACI1") file.remove(i[n])
    if(rows < 5 & type == "TOA5") file.remove(i[n])
  } 
}

## ================================ Function 3: process =========================================
# FUNCTION 3: process(mid_dir)
# Input: 
# mid_dir = intermediate directory

# Output: 
# Writes TOA5 and TOACI1 .dat and .backup files to .csv format. Also changes headers and renames files. 
# After files are processed, they get removed from raw_working and copied into raw_archive
# process() also creates a meta file saved as "[year]_meta.txt", which contains a record of the raw and processed filepaths as well as header names for all files.

process <- function(mid_dir) { 
  # Create raw directory paths
  raw_working <- file.path(mid_dir, "0_raw", "raw_working")
  raw_archive <- file.path(mid_dir, "0_raw", "raw_archive")
  
  # Create processed directory path
  proc_working <- file.path(mid_dir, "1_processed", "proc_working")
  
  # Create list of file names in the raw directory
  i <- list.files(raw_working, pattern = NULL, all.files = FALSE, 
                      full.names = TRUE, recursive = TRUE, 
                      ignore.case = FALSE) 
  # If no files, stop process
  if(length(i) == 0){
    stop("No files to process")
  }
  
  for (n in 1:length(i)) {
    # Read in start of data to determine header and type
    dat_header <- read.table(i[n], sep = ",", quote = "\"", 
                             dec = ".", header = FALSE,
                             na.strings = "NA", colClasses = "character", nrows = 6, 
                             skip = 0, check.names = FALSE,fill=TRUE, 
                             strip.white = FALSE, blank.lines.skip = TRUE) 
    
    # Read in data differently depending on table type
    type <- dat_header[1,1]           
    
    if(type == "TOACI1") dat <- read.table(i[n], sep = ",", quote = "\"'",
                                           dec = ".", na.strings = "NA", colClasses = "character", nrows = -1,
                                           skip = 2, fill=TRUE, strip.white = FALSE, blank.lines.skip = TRUE)
    
    if(type == "TOA5" ) dat <- read.table(i[n], sep = ",", quote = "\"'",
                                          dec = ".", na.strings = "NA", colClasses = "character", nrows = -1,
                                          skip = 4, fill=TRUE, strip.white = FALSE, blank.lines.skip = TRUE)
    
    # Get logger info and column names from header
    loggerinfo <- dat_header[1, ]
    datnames <- dat_header[2, 1:ncol(dat)] 
    datnames <- c(datnames, "Logger", "Program", "Table") 
    
    # Format data tables depending on type
    # This creates new columns that contain info on logger, program, and table
    if(type == "TOA5") dat <- cbind(dat, loggerinfo[2], loggerinfo[6], loggerinfo[8], row.names = NULL) 
    if(type == "TOACI1") dat <- cbind(dat, loggerinfo[2], type, loggerinfo[3], row.names = NULL)
    
    # Set new column names
    colnames(dat) <- datnames 
    
    # Get date and time for file naming purposes
    # date <- substr(dat[nrow(dat), 1], 1, 10) 
    # time <- substr(dat[nrow(dat), 1], 12, 20) 
    date <- substr(dat[1,1], 1, 10)
    time <- substr(dat[1,1], 12, 20)
    time <- chartr(":", "-", time)
    
    # Create unique file name by pulling data from process file in this order:
    # Table type, date, time, block, number of rows in data frame, type of original file 
    # Any file name that could be overwritten would be a duplicate file
    if(type == "TOA5"){
      loggerinfo[1, 8] <- gsub("_", "", loggerinfo[1, 8])
      loggerinfo[1, 2] <- gsub("_", "", loggerinfo[1, 2])
      filename <- paste(loggerinfo[1,8], date, time, loggerinfo[1,2], nrow(dat), type, ".csv", sep="_")
    }
    if(type =="TOACI1"){
      loggerinfo[1, 3] <- gsub("_", "", loggerinfo[1, 3])
      loggerinfo[1, 2] <- gsub("_", "", loggerinfo[1, 2])
      filename <- paste(loggerinfo[1,3], date, time, loggerinfo[1,2], nrow(dat), type, ".csv", sep="_")
    }
    
    # Add the rest of the file path to file name
    filename <- paste(proc_working, "/", filename, sep = "")
    
    # Create metadata and file path for metadata
    meta_dat <- c(filename, datnames, i[n]) 
    meta_file <- paste(proc_working, "/", format(Sys.time(), "%Y-%m-%d"), "_meta.txt", sep = "")
    
    # Save tables including metadata
    write.table(meta_dat, meta_file, append = TRUE, quote = FALSE, sep = ",", 
                na = "NA", dec = ".", row.names = FALSE, 
                col.names = FALSE, qmethod = c("escape", "double")) 
    
    write.table(dat, filename, append = FALSE, quote = FALSE, sep = ",", 
                na = "NA", dec = ".", row.names = FALSE,
                col.names = TRUE, qmethod = c("escape", "double"))
    
    # Copy files to raw_archive folder
    file.copy(i[n], raw_archive)
    
    # Remove files from raw_dir
    file.remove(i[n])
  }
  print("Files have been processed.")
}

## ============================= Function 4: sorter ==============================================
# FUNCTION 4: sorter(mid_dir)
# Input: 
# mid_dir = intermediate directory

# Output: 
# Copies all processed data into the appropriate sub-folders in sorted_working and processed_archive
# Removes processed data from processed_working as it's sorted.

sorter <- function(mid_dir){
  # Create working directories
  proc_working <- file.path(mid_dir, "1_processed", "proc_working")
  proc_archive <- file.path(mid_dir, "1_processed", "proc_archive")
  sort_working <- file.path(mid_dir, "2_sorted", "sorted_working")
  sort_archive <- file.path(mid_dir, "2_sorted", "sorted_archive")
  
  # Create list of file names from the processed directory. 
  # abs_files = absolute file paths, rel_files = relative file paths
  abs_files <- list.files(proc_working, pattern = NULL, all.files = FALSE,
                          full.names = TRUE, recursive = TRUE,
                          ignore.case = FALSE)
  rel_files <- list.files(proc_working, pattern = NULL, all.files = FALSE,
                          full.names = FALSE, recursive = TRUE,
                          ignore.case = FALSE)
  
  if(length(abs_files) == 0){
    stop("No files to sort")
  }
  
  # Save file path of metadata 
  meta <- abs_files[file_ext(abs_files) == "txt"]
  
  # Only include files with a .csv extension
  abs_files <- abs_files[file_ext(abs_files) == "csv"]
  rel_files <- rel_files[file_ext(rel_files) == "csv"]
  
  # Split file names into separate components
  logger_info <- strsplit(rel_files, "_")
  logger_info <- lapply(logger_info, tolower)
  table_type <- vector(mode = "character", length(rel_files))
  logger_name <- vector(mode = "character", length(rel_files))
  
  # Extract table type and logger type from file name
  # Create a list of folder names
  for(n in 1:length(rel_files)){
    table_type[n] <- logger_info[[n]][1]
    logger_name[n] <- logger_info[[n]][4]
  }
  
  # Assign logger names for folders
  for(n in 1:length(logger_name)){
    if(grepl("high", logger_name[n], ignore.case = T)){
      logger_name[n] <- "highmarsh"
    } else if(grepl("low", logger_name[n], ignore.case = T)){
      logger_name[n] <- "lowmarsh"
    } else if(grepl("pioneer", logger_name[n], ignore.case = T)){
      logger_name[n] <- "pioneer"
    }
  }
  
  # Assign table name
  for(n in 1:length(table_type)){
    if(grepl("ndvi", table_type[n], ignore.case = T)){
      table_type[n] <- "ndvi"
    }
  }
  
  # Make sure logger names are decent
  folder_names <- paste(logger_name, table_type, sep = "_")

  # Create folders
  for(n in 1:length(folder_names)){
    if(!dir.exists(file.path(sort_working, folder_names[n]))){
      dir.create(file.path(sort_working, folder_names[n]))
    }
    if(!dir.exists(file.path(sort_archive, folder_names[n]))){
      dir.create(file.path(sort_archive, folder_names[n]))
    }
    # Copy files into corresponding folders in sorted_working
    file.copy(abs_files[n], file.path(sort_working, folder_names[n]))
    # Also copy files to processed_archive once they've been sorted
    file.copy(abs_files[n], proc_archive)
    
    # Remove files from processed_working
    file.remove(abs_files[n])
  }
  # Copy meta.txt to processed_archive and remove from processed_working
  file.copy(meta, proc_archive)
  file.remove(meta)
  
  print("Files have been sorted.")
}

## ============================== Function 5: mean_rm ===========================================
# FUNCTION 5: mean_rm(x)
# Input: 
# x = vector across which you want to find the mean

# Output: 
# Same thing as the mean function, just with the na.rm argument automatically set to TRUE. 
# i.e. takes the mean and automatically removes NA values

mean_rm <- function(x){mean(x, na.rm = T)}

## ============================== Function 6: norm_files ===========================================
# FUNCTION 6: norm_files(mid_dir, logger, table, year)
# Input:
# logger = logger(s) that you want to draw data from. Can be single element or character vector
# table = table(s) that you want to draw data from. Can be single element or character vector
# year = year(s) that you want to get data from. Can be single element or character vector

# Output:
# Returns files that correspond to the tables you want, in preparation for normalization 
# Using this function ensures that you search for only valid loggers and tables

norm_files <- function(mid_dir, logger, table, year) {
  # Get directories for data
  sorted_files <- list.dirs(file.path(mid_dir, "2_sorted", "sorted_working"), full.names = FALSE, recursive = FALSE)
  sorted_files_full <- list.dirs(file.path(mid_dir, "2_sorted", "sorted_working"), full.names = TRUE, recursive = FALSE)
  
  # Create list of possible loggers and tables that can be searched
  possible_loggers <- unique(sapply(strsplit(sorted_files, "_"), "[[", 1))
  possible_tables <- unique(sapply(strsplit(sorted_files, "_"), "[[", 2))
  
  logger <- tolower(logger)
  table <- tolower(table)
  
  # Look for loggers and tables and return messages corresponding to correct combinations
  if((min(logger %in% possible_loggers) == 0) & (min(table %in% possible_tables) == 1)){
    print("That logger doesn't exist in the sorted_working folder. Check your input for typos. The possible loggers to choose from are:")
    print(possible_loggers)
    stop("You entered an invalid logger")
  } 
  if((min(table %in% possible_tables) == 0) & (min(logger %in% possible_loggers) == 1)){
    print("That table doesn't exist in the sorted_working folder. Check your input for typos. The possible tables to choose from are:")
    print(possible_tables)
    stop("You entered an invalid table")
  } 
  if((min(table %in% possible_tables) == 0) & (min(logger %in% possible_loggers) == 0)){
    print("That logger and table don't exist in the sorted_working folder. Check your input for typos. The possible loggers to choose from are:")
    print(possible_loggers)
    print("The possible tables to choose from are:")
    print(possible_tables)
    stop("You entered an invalid logger and table")
  } else {
    dirs_for_norm <- sorted_files_full[grepl(paste(paste0(logger, "_"), collapse = "|"), sorted_files_full, ignore.case = TRUE) == T 
                                       & grepl(paste(table, collapse = "|"), sorted_files_full, ignore.case = TRUE) == T]
  }
  print("The folders you want to normalize are as follows:")
  print(basename(dirs_for_norm))
  
  # Return files that match with the criteria that user inputs
  files_for_norm <- list.files(dirs_for_norm, pattern = year, all.files = FALSE, 
                               full.names = TRUE, recursive = TRUE, 
                               ignore.case = FALSE)
  
  # files_for_norm <- files_for_norm[grepl(paste(as.vector(outer(table, year, paste, sep="_")), collapse = "|"), files_for_norm, ignore.case = TRUE)]
  
  print("The files you want to normalize are as follows:")
  print(files_for_norm)
  return(files_for_norm)
}

##  ========================== Function 8: norm_merit ========================================
# FUNCTION 8: norm_merit(source_dir, norm_dir, table_type, design_table, plot_names, increment)
# The two key files needed for this are:
# 1) design_table: this connects the experimental design to the cr1000 variable name and the scale link for data. 
# 2) plot_names: this contains the experimental design and the treatments in each plot.

# Input:
# source_dir = list of directories in "sorted_working" that you want to read files from. Taken from norm_files function
# norm_dir = directory of normalized files
# design_table = experimental design and link
# plot_names = experimental design
# increment = increment that the data is taken at

# Output: 
# Re-Formats raw data files from loggernet processing, and stacks (melts) the data. 
# Creates a logger file with all logger/site level data, and creates a plot level file with all plot level data
# Also creates renormalized files for data file
# Creates a normalized data file in a folder called "tabletype_year" in the "normal_working" folder
# Also creates a copy of the normalized data file in the "normal_archive" folder


###########Note to Liz from Liz: MAKE SURE that the design table logger and cr1000e_name on the data file is matching witht he design table. 

norm_nordsalt <- function(files_for_norm, mid_dir, design_path, plotname_path, varname_path, increment){
  # Create path to source directory (sorted_working) and normal working directory
  sort_working <- file.path(mid_dir, "2_sorted", "sorted_working")
  sort_archive <- file.path(mid_dir, "2_sorted", "sorted_archive")
  norm_working <- file.path(mid_dir, "3_normal", "norm_working")
  
  # Read in experimental design tables
  design <- fread(design_path, na.strings = "")
  plotname <- fread(plotname_path, na.strings = "")
  varname <- fread(varname_path, na.strings = "")
  
  # Set key for plotname and design tables to speed joining
  designkey <- c("logger", "link")
  setkeyv(plotname, designkey)
  setkeyv(design, designkey)
  
  #create keytables
  plotname <- lapply(plotname,tolower)
  setDT(plotname)
  design<-lapply(design,tolower)
  setDT(design)
  
  # Merge plotname and design to create big table that includes all of the measurements / parameters that apply to each plot
  design <- merge(plotname, design, allow.cartesian = TRUE, by = designkey)
  design <- unique(design)
  setDT(design)
  
  design <- lapply(design, tolower)
  design <- as.data.table(design)
  
  # Also merge variable type with key
  key <- c("research_name")
  setkeyv(varname, key)
  setkeyv(design, key)
  
  varname <- as.data.table(varname)
  
  design <- merge(varname, design, allow.cartesian = TRUE, by = key)
  
  
  # Identify variable types "key", "site", and "plot" level from "var_role" variable in design document
  # key vars
  temp <- design[var_role == "key",]
  key_vars <- as.vector(temp$cr1000_name)
  key_vars <- unique(key_vars)
  
  # logger-scale, chamber-scale,  or site-scale vars
  temp <- design[var_role == "logger",]
  logger_vars <- as.vector(temp$cr1000_name)
  logger_vars <- unique(logger_vars)
  
  # chamber-scale vars
  temp <- design[var_role == "chamber",]
  chamber_vars <-as.vector(temp$cr1000_name)
  chamber_vars <- unique(chamber_vars)
  
  # box-scale vars
  temp <- design[var_role == "box",]
  box_vars <-as.vector(temp$cr1000_name)
  box_vars <- unique(box_vars)
  
  # Load files
  i <- files_for_norm
  
  # Load relative file paths
  j <- basename(files_for_norm)
  
  if(length(i) == 0){
    stop("No files to normalize in the specified directories.")
  }
  
  # for loop for everything
  for (n in 1:length(i)){
    # Read in data table
    dt<-fread(i[n],tz = "")
    
    # Edit column names for ease of use
    setnames(dt, tolower(names(dt)))
    newnames <- names(dt)
    newnames <- gsub("[(]", "", newnames)
    newnames <- gsub("[)]", "", newnames)
    setnames(dt, newnames)
    
    if("POSIXct" %in% class(dt$timestamp)){
      dt$timestamp <- format(as.POSIXct(dt$timestamp, tz="Etc/GMT-1"), 
                             format="%Y-%m-%d %H:%M:%S")
    }
    
    # Make all data lowercase
    dt <- lapply(dt, tolower)
    setDT(dt)
    
    # Create row_id
    dt$row_id <- paste("nordsalt", dt$timestamp, dt$logger, sep = "_")
    
    #create logger column 
    name <- j[n]
    logger_name <- tolower(substr(name,28,42))
    dt$logger <- logger_name
    
    # Format time
    Sys.setenv(TZ = "Etc/GMT-1") ### time is already in GMT +1 all year long. 
    dt$timestamp <- as.POSIXct(dt$timestamp, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
    
    # Format time so that minute is rounded to the nearest X-minute interval
    ## create time2 and row_id
    minute_time <- round(minute(dt$timestamp)/increment)*increment
    dt$time2 <- update(dt$timestamp, min = minute_time)
    
    # Separate data into transect_level, chamber level and site level by selecting relevant variables
    logger_names <- c(key_vars, "logger", "time2", "row_id",logger_vars)
    chamber_names <- c(key_vars, "logger", "time2","row_id", chamber_vars)		
    box_names <- c(key_vars, "logger", "time2","row_id", box_vars)
    
    # Separate data into plot level and site level by selecting relevant variables
    dt_chamber <- subset(dt, select = names(dt) %in% chamber_names) ##site data for merge
    dt_box <- subset(dt, select = names(dt) %in% box_names) ##plot level to denormalize
    
    ### Normalize data 
    # Stack block/chamber data (long form data)
    id_vars <- c("logger", "time2","row_id","timestamp")
    
    dt_chamber2 <- melt(dt_chamber, id.vars = id_vars, variable.name = "cr1000_name", na.rm = FALSE)
    setDT(dt_chamber2)
    
    # Stack box data
    dt_box2 <- melt(dt_box, id.vars = id_vars, variable.name = "cr1000_name", na.rm = FALSE)
    setDT(dt_box2)

      # Merge box level variables with design table
      merge_key <- c("logger", "cr1000_name")
      setkeyv(dt_box2, merge_key)
      setkeyv(design, merge_key)
      design <- as.data.table(design)
      dt_box3 <- merge(dt_box2, design, allow.cartesian = TRUE, by = merge_key)
      
      # Select only the variables we want. 
      # Include all measurement variables and plot-associated variables.
      dt_box4 <- subset(dt_box3, select = c("row_id", "time2", "timestamp", "logger", "plotid", 
                                            "value", "research_name", "cr1000_name", "type"))
      
      # Separate data into separate types (numeric and character) because you need to deal with duplicates in different ways 
      dt_box4_num <- dt_box4[type == "numeric",]
      dt_box4_char <- dt_box4[type == "character",]
      
      # Reformat numeric data -- choose unique values, set value as numeric
      setkey(dt_box4_num, NULL)
      dt_box4_num <- unique(dt_box4_num)
      dt_box4_num <- dt_box4_num
      dt_box4_num$value <- as.double(dt_box4_num$value)
      is.na(dt_box4_num) <- is.na(dt_box4_num)
      
      # Create formula for dcast based on all plot variables and key variables
      f <- as.formula(plotid+row_id+logger+time2 ~ research_name)
      
      # Recast numeric data to wide format with each variable as its own column. Note that fill should be allowed to default
      dt_box_num_wide <- dcast(dt_box4_num, f, fun.aggregate = mean_rm, subset = NULL, drop = TRUE, value.var = "value") 
      dt_box_num_wide <- as.data.table(dt_box_num_wide)
      
      # If there are character variables, cast character table to wide format and join with numeric data
      if(nrow(dt_box4_char) > 0){
        # Set up the character data table for cast
        setkey(dt_box4_char, NULL)
        dt_box4_char <- unique(dt_box4_char)
        is.na(dt_box4_char) <- is.na(dt_box4_char)
        
        # Cast to wide
        # dt_box_char_wide <- dcast(dt_box4_char, f, fun.aggregate = NULL, subset = NULL, drop = TRUE, value.var = "value") # note that fill should be allowed to default
        dt_box_char_wide <- dcast(dt_box4_char, f, fun.aggregate = function(x){unique(x)[1]}, subset = NULL, drop = TRUE, value.var = "value")
        dt_box_char_wide <- as.data.table(dt_box_char_wide)
        
        # Set keys for merge
        mergekey <- c("logger", "row_id", "time2", "plotid")
        setkeyv(dt_box_num_wide, mergekey)
        setkeyv(dt_box_char_wide, mergekey)
        
        # Merge numeric and character data
        dt_box_merge <- dt_box_num_wide[dt_box_char_wide]
        
      } else{
        # If no character data, then merge table = numeric table
        dt_box_merge <- dt_box_num_wide
      }
      designkey <- c("logger", "cr1000_name")
      
      # Prep chamber data
      dt_chamber2 <- unique(dt_chamber2)
      # Merge chamber data with design so that we can get the research name
      dt_chamber3 <- merge(dt_chamber2, design, allow.cartesian = TRUE, by = designkey)
      
      # Select only the variables we want
      dt_chamber4 <- subset(dt_chamber3, select = c("row_id", "chamber", "time2", "timestamp", "logger", "plotid", 
                                                    "value", "research_name", "cr1000_name", "type")) #still working here
  
      # Cast chamber variables to wide
      dt_chamber_wide <- dcast(dt_chamber4, row_id+logger+time2+plotid ~ research_name, fun.aggregate = function(x){unique(x)[1]}, 
                               subset = NULL, drop = TRUE, value.var = "value") 
      
      # Prepare everything for merge
      mergekey <- c("row_id","logger","time2","plotid")
      
      dt_box_merge$time2 <- as.POSIXct(dt_box_merge$time2)
      dt_chamber_wide$time2 <- as.POSIXct(dt_chamber_wide$time2)
      
      #remove the temp_avg and temp_raw_std_avg variables from both data tables (may remove this later)
      dt_box_merge <- dt_box_merge[,!"temp_avg"]
      dt_box_merge <- dt_box_merge[,!"temp_raw_std"]
      
      dt_chamber_wide <- dt_chamber_wide[,!"temp_avg"]
      dt_chamber_wide <- dt_chamber_wide[,!"temp_raw_std"]
      
      setDT(dt_box_merge)
      setDT(dt_chamber_wide)
      setkeyv(dt_box_merge, mergekey)
      setkeyv(dt_chamber_wide, mergekey)
      
      ### merge non normalized variables with data
      dt_normalized <- dt_chamber_wide[dt_box_merge]
  
    
    # Now need to add in all the plot variables. I think this makes it faster?
    # (Faster versus adding it in earlier while casting)
    key <- c("plotid", "logger")
    setDT(dt_normalized)
    setkeyv(dt_normalized, key)
    
    add <- c("logger", "plotid", "treatment", "grazed_ungrazed","origin","transect", "transect_s_treatment")
    
    add_plot_vars <- as.data.table(unique(subset(plotname, select = add)))
    add_plot_vars$plotid <- as.character(add_plot_vars$plotid)
    setkeyv(add_plot_vars, key)
    dt_normalized <- add_plot_vars[dt_normalized]
    
    # Select key variables and then order all columns alphabetically?
    dt_normalized <- dt_normalized %>%
      select(logger, plotid, treatment, origin, grazed_ungrazed, transect, transect_s_treatment, row_id, time2, 
             (order(colnames(dt_normalized[, 10:ncol(dt_normalized), with = F]))+9))
    
    # column selection
    default_cols <- c("logger", "plotid","treatment", "grazed_ungrazed","origin", "transect", "transect_s_treatment",
                      "row_id", "time2", "program", "record", "table")
    
    if(all(colnames(dt_normalized) %in% default_cols)){
      stop("There are no data columns -- check that your data matches with your design document")
    } else{
      
      # Format timestamp nice
      dt_normalized$time2 <- format(as.POSIXct(dt_normalized$time2, tz="Etc/GMT-1"), 
                                    format="%Y-%m-%d %H:%M:%S")
      
      if(sum(is.na(dt_normalized$time2)) > 0){
        print(paste0("There are NA timestamps. Check ", j[n]))
      }
      
      # Create new file name
      file_name <- paste0("norm_", j[n])
      file_name <- file.path(norm_working, file_name)
      
      # Save table to norm_working
      write.table(dt_normalized, file_name, append = FALSE, quote = FALSE, sep = ",",
                  na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))
      
      # Is this useful? Yes, I guess so --- tells you when it's done with each file. 
      print(paste0("File ", n, "/", length(i), " done"))
      
      # Copy original file to sorted_archive
      sort_dir <- unlist(strsplit(i[n], "/sorted_working/"))
      folder <- unlist(strsplit(sort_dir[2], "/"))[1]
      copy_dir <- file.path(sort_archive, folder)
      
      file.copy(i[n], copy_dir)
      file.remove(i[n])
    }
  }
}


## ============================== Function 9: monthly_files =============================================
# FUNCTION 9: monthly_files(table, year)
# Input:
# table = table(s) that you want to draw data from. Can be single element or character vector
# year = year(s) that you want to draw data from. Can be single element or vector.

# Output:
# Returns the files that correspond to the tables you want, in preparation for monthly aggregation 
# Using this function ensures that you search for only valid tables and years
monthly_files <- function(mid_dir, table) {
  # List files from normalized directories 
  norm_files <- list.files(file.path(mid_dir, "3_normal", "norm_working"), pattern = NULL, all.files = FALSE,
                           full.names = FALSE, recursive = FALSE,
                           ignore.case = FALSE)
  norm_files_full <- list.files(file.path(mid_dir, "3_normal", "norm_working"), pattern = NULL, all.files = FALSE,
                                full.names = TRUE, recursive = FALSE,
                                ignore.case = FALSE)
  
  # Search for possible tables
  possible_tables <- tolower(unique(sapply(strsplit(norm_files, "_"), "[[", 2)))
  
  table <- tolower(table)
  
  if(min(table %in% possible_tables) == 0){
    print("That table doesn't exist in the normal_working folder. Check your input for typos. The possible tables to choose from are:")
    print(possible_tables)
    stop("You entered an invalid table")
  } else {
    dirs_for_month <- norm_files_full[grepl(paste(table, collapse = "|"), norm_files_full, ignore.case = T) == T]
  }
  
  print("The files you want to aggregate by month are as follows:")
  print(dirs_for_month)
  return(dirs_for_month)
}

##  =========================== Function 10: monthlymanage =======================================
# FUNCTION 10: monthlymanage(source_files, monthly_dir, out_path, logger_role)
# Input:
# source_files = list of normalized files
# monthly_dir = directory for storage of monthly_working files
# out_path = path for folder where everything is saved for researcher use
# logger_role = document connecting logger or table name with the project it's associated with

# Output: 
# Divides data table into separate monthly files. Accounts for duplicates.
# If monthly file already exists, data are appended to the existing file

monthlymanage <- function(source_files, mid_dir){
  # Set up working directories
  norm_working <- file.path(mid_dir, "3_normal", "norm_working")
  norm_archive <- file.path(mid_dir, "3_normal", "norm_archive")
  month_working <- file.path(mid_dir, "4_monthly", "monthly_working")

  if(length(source_files) == 0){
    stop("There are no files to aggregate that match your specified table and year")
  }
  
  for (n in 1:length(source_files)){
    # Read in data table 
    dt <- read_csv(source_files[n])#, tz = "")
    
    # Format time
   # if(length(which(!grepl("20..-..-.. ..:..:..", dt$time2))) == 0){
      Sys.setenv(TZ = "Etc/GMT-1") ### set for EST all year long
      dt$time2 <- as.POSIXct(dt$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
   # } else {
   #   print(paste0("These times are formatted incorrectly: ", dt$time2[which(!grepl("20..-..-.. ..:..:..", dt$time2))]))
   # }
    dt <- dt %>% drop_na(time2)
      
    # Create year, month, and logger variables to create a key data frame
    # Create a different table for every combination of month, year, and logger
    dates <- unique(format(dt$time2, "%Y-%m"))
    dates <- strsplit(dates, "-")
    logger <- unique(tolower(dt$logger))
    y2 <- data.frame(yr = as.integer(sapply(dates, "[[", 1)),
                     mo = as.integer(sapply(dates, "[[", 2)), 
                     logger = logger, row.names = NULL)
    
    # Get table type
    table_splt <- strsplit(source_files[n], "/")
    table2 <- strsplit(table_splt[[1]][length(table_splt[[1]])], "_")
    table <- table2[[1]][2]
    
    # Get output directory
    output_dir <- month_working

    for (m in 1:nrow(y2)) {
      # Subset data by month and save each one as a different file
      dtplot <- subset(dt, month(dt$time2) == y2$mo[m] & dt$logger == y2$logger[m] & year(dt$time2) == y2$yr[m])
      
      # Created this to account for logger name change
      if(y2$logger[m] == "cr1000_merit_highmarsh" | y2$logger[m] == "merit highmarsh"){
        path <- paste("highmarsh", table, y2$mo[m], y2$yr[m], sep = "_")
      } else if(y2$logger[m] == "merit lowmarsh"){
        path <- paste("lowmarsh", table, y2$mo[m], y2$yr[m], sep = "_")
        } else if (y2$logger[m] == "merit pioneer"){
          path <- paste("pioneer", table, y2$mo[m], y2$yr[m], sep = "_")
          } else{
        path <- paste(gsub("_", "", y2$logger[m]), table, y2$mo[m], y2$yr[m], sep = "_")
      }
      path <- paste(path, "csv", sep = ".")
      path_out <- paste(output_dir, path, sep = "/")

      # If there's already a data table with the same name, bind rows together and overwrite
      if (file.exists(path_out) == TRUE) {
        dt2 <- fread(path_out, tz = "")
        
        if(length(which(!grepl("20..-..-.. ..:..:..", dt2$time2))) == 0){
          Sys.setenv(TZ = "Etc/GMT-1") ### set for EST all year long
          dt2$time2 <- as.POSIXct(dt2$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
        } else {
          print(paste0("These times are formatted incorrectly: ", dt2$time2[which(!grepl("20..-..-.. ..:..:..", dt2$time2))]))
        }
        
        dt2 <- plyr::rbind.fill(dtplot, dt2)
        dt2 <- unique(dt2)
        dt2 <- distinct_at(dt2, vars(-program), .keep_all = T)
        dt2 <- as.data.table(dt2)

        # Format time for output
        dt2$time2 <- format(as.POSIXct(dt2$time2, tz="Etc/GMT-1"), 
                               format="%Y-%m-%d %H:%M:%S")

        write.table(dt2, path_out, append = FALSE, quote = FALSE, sep = ",",
                    na = "NA", dec = ".", row.names = FALSE,
                    col.names = TRUE, qmethod = c("escape", "double"))
        
        # Otherwise, if the data table doesn't exist yet, just save it to the output dir
      } else if (file.exists(path_out) == FALSE) {
        # Delete duplicates
        dtplot <- unique(dtplot)
        dtplot <- distinct_at(dtplot, vars(-program), .keep_all = T)
        # Sort by time
        dtplot <- arrange(dtplot, time2)

        # Format time for output
        dtplot$time2 <- format(as.POSIXct(dtplot$time2, tz="Etc/GMT-1"), 
                            format="%Y-%m-%d %H:%M:%S")
        
        write.table(dtplot, path_out, append = FALSE, quote = FALSE, sep = ",",
                    na = "NA", dec = ".", row.names = FALSE,
                    col.names = TRUE, qmethod = c("escape", "double"))
      }
    }
    # Is this useful? Yes, I guess so --- tells you when it's done with each file. 
    print(paste0("File ", n, "/", length(source_files), " done"))
    
    # Copy file to norm_archive
    file.copy(source_files[n], norm_archive)
    # Remove file from norm_working
    file.remove(source_files[n])
  }
}


# ======================= Function 11: bundle ========================================
# FUNCTION 11: bundle(monthly_dir, out_path, logger_role)
# Input:
# monthly_dir = directory where monthly files are stored that haven't been aggregated into a yearly file yet
# out_path = path for folder where everything is saved for researcher use
# logger_role = document connecting logger or table name with the project it's associated with

# Output: 
# Aggregates all monthly data into one yearly data file

# bundle <- function(mid_dir, logger, table){
bundle <- function(mid_dir){
  # Get working directories
  monthly_working <- file.path(mid_dir, "4_monthly", "monthly_working")
  monthly_archive <- file.path(mid_dir, "4_monthly", "monthly_archive")
  yearly_dir <- file.path(mid_dir, "5_yearly")
  
  # Create list of file paths in source directory
  i <- list.files(monthly_working, pattern = NULL, all.files = FALSE,
                  full.names = TRUE, recursive = TRUE,
                  ignore.case = T)
  # i <- list.files(monthly_working, pattern = paste0(logger, "_", table), all.files = FALSE,
  #                 full.names = TRUE, recursive = TRUE,
  #                 ignore.case = T)
  
  # Get basenames of all the files
  j <- basename(i)
  
  # Separate data based on logger, table type, and year
  info <- strsplit(j, "_")
  logger <- tolower((sapply(info, "[[", 1)))
  table <- tolower((sapply(info, "[[", 2)))
  year <- substr(sapply(info, "[[", 4), 1, 4)
  
  # Create dataframe with all logger, table, year combinations
  lt_id <- data.frame(logger = logger, 
                      table = table, 
                      year = year,
                      id = paste(logger, table, year, sep = "_"))
  
  lt_id <- unique(lt_id)
  rownames(lt_id) <- 1:nrow(lt_id)
  
  # For each combination of logger, table, and year...
  for(n in 1:nrow(lt_id)){
    # Select files that contain logger name
    group <- i[grepl(lt_id$logger[n], i, ignore.case = TRUE) & grepl(lt_id$table[n], i, ignore.case = TRUE) & grepl(lt_id$year[n], i)]
    
    # Read in first data table
    dt <- fread(group[1], tz = "")
    
    # For each group...
    if(length(group) > 1){
      for (m in 2:length(group)){
        # Append all other data tables to the first one
        dt2 <- fread(group[m], tz = "")
        dt <- rbind.fill(dt, dt2)
      }
    }
    
    # Run unique function and order by time2
    dt <- as.data.table(dt)
    dt <- unique(dt)
    dt <- distinct_at(dt, vars(-program), .keep_all = T)
    
    #if(length(which(!grepl("20..-..-.. ..:..:..", dt$time2))) == 0){
      Sys.setenv(TZ = "Etc/GMT-1") ### set for EST all year long
      dt$time2 <- as.POSIXct(dt$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
    #} else {
    #  print(paste0("These times are formatted incorrectly: ", dt$time2[which(!grepl("20..-..-.. ..:..:..", dt$time2))]))
    #}
    
    dt <- dt[order(time2)]

    # Create output filename
    output_dir <- yearly_dir

    newpath <- paste0(output_dir, "/", lt_id$logger[n], "_", lt_id$table[n], "_", lt_id$year[n], ".csv")
    
    # If file exists already, append it to the existing one and overwrite. If it doesn't exist, save it normally
    if (file.exists(newpath) == TRUE){
      # Read in existing data, use rbind.fill, get unique values only, 
      dt2 <- fread(newpath, tz = "")
      
      # Set time format for dt2
      if(length(which(!grepl("20..-..-.. ..:..:..", dt2$time2))) == 0){
        Sys.setenv(TZ = "Etc/GMT-1") ### set for EST all year long
        dt2$time2 <- as.POSIXct(dt2$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
      } else {
        print(paste0("These times are formatted incorrectly: ", dt2$time2[which(!grepl("20..-..-.. ..:..:..", dt2$time2))]))
      }
      
      # rbind with dt
      dt2 <- plyr::rbind.fill(dt, dt2)
      dt2 <- unique(dt2)
      # Get unique values for data with the exception of the program column -- sometimes the program changes and you get duplicate data
      dt2 <- distinct_at(dt2, vars(-program), .keep_all = T)
      dt2 <- as.data.table(dt2)
      # Order by time2
      dt2 <- dt2[order(dt2$time2),]
      
      # Print warning if there are duplicate times but different data
      if(sum(duplicated(dt2[, c("time2", "plotid", "id", "zone", "treatment", "transect", "site_x_treatment", "logger")])) > 0){
        check <- dt2[duplicated(dt2[, c("time2", "plotid", "id", "zone", "treatment", "transect", "site_x_treatment", "logger")]),]
        dup_times <- unique(check$time2)
        print(paste0("These times are duplicated but have different data", dup_times))
      }
      
      # Format time for output
      dt2$time2 <- format(as.POSIXct(dt2$time2, tz="Etc/GMT-1"), 
                          format="%Y-%m-%d %H:%M:%S")

      # Write file
      write.table(dt2, newpath, append = FALSE, quote = FALSE, sep = ",",
                  na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))
    }
    if (file.exists(newpath) == FALSE){
      # Print warning if there are duplicate times but different data
      if(sum(duplicated(dt[, c("time2", "plotid", "id", "zone", "treatment", "transect", "site_x_treatment", "logger")])) > 0){
        check <- dt[duplicated(dt[, c("time2", "plotid", "id", "zone", "treatment", "transect", "site_x_treatment", "logger")]),]
        dup_times <- unique(check$time2)
        print(paste0("These times are duplicated but have different data", dup_times))
      }
      
      # Format time for output
      dt$time2 <- format(as.POSIXct(dt$time2, tz="Etc/GMT-1"), 
                         format="%Y-%m-%d %H:%M:%S")
      
      write.table(dt, newpath, append = FALSE, quote = FALSE, sep = ",",
                  na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))
    }
    # Is this useful? Yes, I guess so --- tells you when it's done with each file. 
    print(paste0(n, "/", nrow(lt_id), " done"))
    
    # Remove old files from the "monthly_working" folder
    file.copy(group, monthly_archive)
    file.remove(group)
    
  }
}

# ============================= Function 13: create_met_export ======================================
# FUNCTION 13: create_met_export(source_dir, table, output_dir)
# Input:
# source_dir = directory where yearly files are stored 
# output_dir = output directory

# Output: 
# Aggregates all export files for each zone and year with the highmarsh met file for that year

create_met_export <- function(source_dir, output_dir, year){
  # Get met files
  met_files <- list.files(source_dir, pattern = paste0("highmarsh_met_", year), all.files = FALSE,
                          full.names = TRUE, recursive = TRUE,
                          ignore.case = FALSE)
  # Get export files
  export_files <- list.files(source_dir, pattern = paste0("export_", year), all.files = FALSE,
                             full.names = TRUE, recursive = TRUE,
                             ignore.case = FALSE)
  export_files <- export_files[!grepl("met_export", export_files)]
  
  for(n in 1:length(export_files)){
    # Get base file names
    export_base <- basename(export_files[n])
    # Get separate table information
    types <- unlist(strsplit(export_base, "_"))
    # Get unique years 
    year <- substr(types[length(types)], 1, 4)
    
    # Get met file
    met <- met_files[grepl(year, met_files)]
    # If met file exists, join
    if(length(met) == 1){
      # Read in met and export files
      met_dt <- fread(met, tz = "")
      export_dt <- fread(export_files[n], tz = "")
      
      # Set time format for dt2
      if(length(which(!grepl("20..-..-.. ..:..:..", met_dt$time2))) == 0){
        Sys.setenv(TZ = "Etc/GMT-1") ### set for EST all year long
        met_dt$time2 <- as.POSIXct(met_dt$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
      } else {
        print(paste0("These times are formatted incorrectly: ", met_dt$time2[which(!grepl("20..-..-.. ..:..:..", met_dt$time2))]))
      }
      
      if(length(which(!grepl("20..-..-.. ..:..:..", export_dt$time2))) == 0){
        Sys.setenv(TZ = "Etc/GMT-1") ### set for EST all year long
        export_dt$time2 <- as.POSIXct(export_dt$time2, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-1")
      } else {
        print(paste0("These times are formatted incorrectly: ", export_dt$time2[which(!grepl("20..-..-.. ..:..:..", export_dt$time2))]))
      }
      
      # Change met_dt to just be site level
      met_dt <- met_dt[,-c("logger", "id", "plotid", "treatment", "zone", "transect", "site_x_treatment", "row_id")]
      met_dt2 <- unique(met_dt)
      
      # Set keys for join for met and export files
      setkey(met_dt2, "time2")
      setkey(export_dt, "time2")
      
      # Join met and export files
      # Repeating vars: cs456_temp_c_1_max, level_cm_avg, level_cm_max, level_cm_min, program, table
      dt_merged <- merge(export_dt, met_dt2, by = "time2", suffixes = c("_export", "_met"), all.x = T)
      
      # Create file name
      filename <- paste0(types[1], "_combined_met_export_", year, ".csv")
      out_path <- file.path(output_dir, filename)
      
      # Format timestamp
      dt_merged$time2 <- format(as.POSIXct(dt_merged$time2, tz="Etc/GMT-1"), format="%Y-%m-%d %H:%M:%S")
      
      # Save table
      write.table(dt_merged, out_path, append = FALSE, quote = FALSE, sep = ",",
                  na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))
    } else{
      # Print this messsage if met file doesn't exist
      print(paste0("No met file for ", year, ". Could not create combined met_export table."))
    }
  }
}


# ============================ Function 12: combiner =======================================
# FUNCTION 12: combiner(source_dir, table, output_dir)
# Input:
# source_dir = directory where yearly files are stored 
# table = table type that you want to aggregate
# output_dir = output directory

# Output: 
# Aggregates all files for a certain table type and year together (collapses the zone separation)

combiner <- function(source_dir, table, pattern, output_dir){
  # Read in files
  yearly_files <- list.files(source_dir, pattern = pattern, all.files = FALSE,
                             full.names = TRUE, recursive = TRUE,
                             ignore.case = FALSE)
  # Add a stop in case there are no tables available
  if(length(yearly_files) == 0){
    print("There are no files for that table in your source directory. Check your input for typos.")
    stop("No files available")
  }
  # Get base file names
  files_base <- basename(yearly_files)
  
  # Get separate table information
  types <- strsplit(files_base, "_")
  
  # Get unique years 
  year <- unique(substr(sapply(types, "[[", length(types[[1]])), 1, 4))
  
  for(n in 1:length(year)){
    # Get files for each year
    year_group <- yearly_files[grepl(year[n], yearly_files)]
    # Create list for for loop
    year_list <- vector(mode = "list", length = length(year_group))
    # Read in all files into the list
    for(m in 1:length(year_group)){
      year_list[[m]] <- fread(year_group[m], tz = "")
    }
    # Bind all tables in list together
    year_output <- rbindlist(year_list, use.names = TRUE, fill = T)
    # Create output filename
    filename <- paste0("MERIT", "_", table, "_", year[n], ".csv")
    filename <- file.path(output_dir, filename)
    
    # No need to format timestamp
    
    # Save table
    write.table(year_output, filename, append = FALSE, quote = FALSE, sep = ",",
                na = "NA", dec = ".", row.names = FALSE,
                col.names = TRUE, qmethod = c("escape", "double"))
  }
}










