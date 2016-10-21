library("data.table")
library("foreign")
#library("plyr")
library("dplyr")
library("tidyr")
library("ggplot2")
library("readr")
#library("lubridate")
library("timeDate")
#library("bizdays")
library("ggthemes")
library("Hmisc")
library("stringr")


setwd("C:/Users/jcolema3/Documents/Big Data Innovation Team/Work/Peak Clearance/Bluetooth Analysis")

##########################FUNCTIONS################################

#function to merge multiple files in a single directory into one data frame
load_data <- function(mypath, names){
  filenames <- list.files(path=mypath, full.names=TRUE)
  datalist <- ldply(filenames, function(fname) {
    dum <- read_csv(file = fname, col_names = names, skip = 1 )
    dum$filename <- basename(fname)
    return(dum) } ) 
}


##########################START################################

colnames <- c("datetime1", "datetime2", "UserId","StartPointNumber", "StartPointName", 
              "EndPointNumber", "EndPointName", "AnalysisId", "MeasuredTime", "MeasuredTimeNoFilter", 
              "ClassOfDevice", "DeviceClass", "outlierLevel", "Both")

#For Gardiner DVP - IGONORE
#colnames2 <- c("datetime1", "datetime2", "StartPointNumber", "StartPointName", 
              "EndPointNumber", "EndPointName", "MinMeasuredTime", "MaxMeasuredTime", "AvgMeasuredTime", 
              "MedianMeasuredTime", "SampleCount", "AccuracyLevel", "ConfidenceLevel")

#temp2 <- load_data(file.path( getwd(), "GardDVP"), colnames2) 
#bliptrack <- temp2 %>%
#  mutate(timestamp = as.POSIXct(strptime( paste(datetime1, datetime2), "%b %d %Y %r"))) %>%
#  select(timestamp, StartPointNumber:filename)

#write_csv(bliptrack, "GardDVPBluetooth20152016.csv")


temp <- load_data(file.path( getwd(), "RawDataIndividual"), colnames) 


#set before/after dates by corridor
queen_buffer <- as.POSIXct(strptime("2015-12-01", "%Y-%m-%d"))
queen_after <- as.POSIXct(strptime("2015-12-01", "%Y-%m-%d"))
dundas_buffer <- as.POSIXct(strptime("2015-12-09", "%Y-%m-%d"))
dundas_after <- as.POSIXct(strptime("2015-12-09", "%Y-%m-%d"))
college_buffer <- as.POSIXct(strptime("2015-12-09", "%Y-%m-%d"))
college_after <- as.POSIXct(strptime("2015-12-09", "%Y-%m-%d"))
other_buffer <- as.POSIXct(strptime("2015-12-02", "%Y-%m-%d"))
other_after <- as.POSIXct(strptime("2015-12-09", "%Y-%m-%d"))
s
#fix timestamp and add corridor and before/after stamp from filename
bt_data <- temp %>%
  mutate(timestamp = as.POSIXct(strptime( paste(datetime1, datetime2), "%b %d %Y %r")),
         date = as.Date(timestamp),
         corridor = sub(".*DT-_(.*)_-_.*_.*_.*_ut_.*", "\\1", filename),
         time_bin = as.numeric(format(timestamp, "%H"))*100 +trunc(as.numeric(format(timestamp, "%M"))/30)+1,  
         segment = paste(StartPointName, EndPointName),
         bef_aft = ifelse(corridor == "Queen" & timestamp < queen_buffer, "Before", 
                    ifelse(corridor == "Queen" & timestamp < queen_after, "Buffer",
                    ifelse(corridor == "Queen" & timestamp >= queen_after, "After",
                    ifelse(corridor == "Dundas" & timestamp < dundas_buffer, "Before",
                    ifelse(corridor == "Dundas" & timestamp < dundas_after, "Buffer",
                    ifelse(corridor == "Dundas" & timestamp >= dundas_after, "After",
                    ifelse(corridor == "College" & timestamp < college_buffer, "Before",
                    ifelse(corridor == "College" & timestamp < college_after, "Buffer",
                    ifelse(corridor == "College" & timestamp >= college_after, "After",
                    ifelse(timestamp < other_buffer, "Before",
                    ifelse(timestamp < other_after, "Buffer", "After"))))))))))),
         start_corridor = sub("(.*)[0-9]", "\\1", StartPointName),
         end_corridor = sub("(.*)[0-9]", "\\1", EndPointName),
         start_point = strtoi(sub(".*([0-9])", "\\1", StartPointName)),
         end_point = strtoi(sub(".*([0-9])", "\\1", EndPointName)),
         start_street = ifelse(StartPointName == "AC1", "Dovercourt",
                        ifelse(StartPointName == "AC2", "Bathurst",
                        ifelse(StartPointName == "AC3", "University",
                        ifelse(StartPointName == "AC4", "Parliament",
                        ifelse(StartPointName == "AD1", "Roncesvalles",
                        ifelse(StartPointName == "AD2", "Sterling",
                        ifelse(StartPointName == "AD3", "Dovercourt",
                        ifelse(StartPointName == "AD4", "Spadina",
                        ifelse(StartPointName == "AQ1", "Dufferin",
                        ifelse(StartPointName == "AQ2", "Bathurst",NA)))))))))),
         end_street = ifelse(EndPointName == "AC1", "Dovercourt",
                        ifelse(EndPointName == "AC2", "Bathurst",
                        ifelse(EndPointName == "AC3", "University",
                        ifelse(EndPointName == "AC4", "Parliament",
                        ifelse(EndPointName == "AD1", "Roncesvalles",
                        ifelse(EndPointName == "AD2", "Sterling",
                        ifelse(EndPointName == "AD3", "Dovercourt",
                        ifelse(EndPointName == "AD4", "Spadina",
                        ifelse(EndPointName == "AQ1", "Dufferin",
                        ifelse(EndPointName == "AQ2", "Bathurst",NA)))))))))),
         seg_desc = paste(start_street, "-", end_street),
         direction = ifelse(start_corridor == end_corridor, 
                            ifelse(start_point > end_point, "WB","EB"),
                            ifelse(start_corridor == "AQ", "NB",
                                   ifelse(start_corridor == "AD", "NB", "SB"))),
         seg_desc2 = ifelse(direction == "EB", paste(start_street, "-", end_street), 
                            paste(end_street, "-", start_street))) %>%
  select(timestamp, date, UserId:Both, corridor, time_bin, segment, seg_desc, seg_desc2, start_corridor, end_corridor, 
         start_point, end_point, start_street, end_street, bef_aft,direction)

#Read in Bluettoth ClassOfDeviec Lookup Table
BTLookup <- read_delim("Bluetooth_ClassOfDevice.txt", delim = "\t")
BTLookup$ClassOfDevice <- strtoi(BTLookup$CoDHex)

write_csv(BTLookup, "BT_ClassOfDevice.csv")

#Join lookup table
bt_data <- bt_data %>%
  left_join(BTLookup, by = c("ClassOfDevice" = "ClassOfDevice")) %>%
  select(timestamp:direction, DeviceType:MajorDeviceClass)
  

end_date <- as.POSIXct(strptime("2015-12-22", "%Y-%m-%d"))
end_time <- as.POSIXct(strptime("2015-12-01", "%Y-%m-%d"))
selected_date <- as.Date("2015-11-28")

myFunc = function(x) {
  result = c(mean(x) - sd(x), mean(x) + sd(x))
  names(result) = c("ymin", "ymax")
  result
}


bt_data$segmentfac <- factor(bt_data$segment, labels = seg_labels, levels = seg_levels)
bt_data$segmentfac <- factor(bt_data$segment, labels = seg_labels, levels = seg_levels)


bt_data %>%
  group_by(corridor, segment) %>%
  summarise(first = min(timestamp))


bt_data_subset <-  filter(bt_data, corridor == "College" | corridor == "Dundas" | corridor == "Queen", 
         DeviceClass == 0,
         time_bin == 901 | time_bin == 902 | time_bin == 1501 | time_bin == 1502 | time_bin == 1801 | time_bin == 1802,
         outlierLevel == 1,
         timestamp < end_date,
         between(wday(bt_data$timestamp),1,5))

summary_stats <- bt_data_subset %>%
  group_by(time_bin, segment, bef_aft) %>%
  summarise(avg = mean(MeasuredTime),
            sample = n(),
            stdev = sd(MeasuredTime),
            coefvar = stdev/avg,
            P05 = quantile(MeasuredTime, probs=0.05),
            P25 = quantile(MeasuredTime, probs=0.25),
            P50 = quantile(MeasuredTime, probs=0.5),
            P75 = quantile(MeasuredTime, probs=0.75),
            P95 = quantile(MeasuredTime, probs=0.95),
            BTI = P95/avg) %>%
  gather(stat, value, avg:BTI) %>%
  unite(stat_ba, bef_aft, stat) %>%
  spread(stat_ba, value)
write_csv(summary_stats, "stats.csv")




plot_data <- bt_data %>%
  filter(corridor == "College", 
         segment == "AC1 AC2" | segment ==  "AC2 AC1",
         DeviceClass == 0,
         outlierLevel == 1,
         time_bin == 901 | time_bin == 902 | time_bin == 1502 | time_bin == 1801,
         timestamp < end_date,
         between(wday(bt_data$timestamp),1,5))
  #summarise(med = median(MeasuredTime), avg = mean(MeasuredTime), min = min(MeasuredTime), max = max(MeasuredTime))

plot_data$time_bin2 <- factor(plot_data$time_bin, labels = c("9:00 - 9:30 EB", "9:30 - 10:00 EB", "15:30 - 16:00 WB", "18:00 - 18:30 WB"))
means <- aggregate(MeasuredTime/60 ~  as.Date(timestamp,format="%b %d %Y"), plot_data, mean)
  
#ggplot(plot_data, aes(x = as.Date(timestamp,format="%b %d %Y"), y = MeasuredTime/60, col = bef_aft, group = as.Date(timestamp,format="%b %d %Y") )) +
ggplot(plot_data, aes(x = as.Date(timestamp,format="%b %d %Y"), y = MeasuredTime/60, col = bef_aft, group = bef_aft)) +
#  ggplot(plot_data, aes(x = bef_aft, y = MeasuredTime/60, col = bef_aft)) +
  
  #ggplot(plot_data, aes(x = factor(bef_aft), y = MeasuredTime/60, col = bef_aft)) +  
  geom_point(size = 2, alpha = 0.4) +
  geom_boxplot(alpha = 0.4) +
  #stat_summary(fun.data = mean_sdl, fun.args = list(mult = 1)) +
  #geom_violin(alpha = 0.4, aes(group = bef_aft)) +
  #geom_violin(alpha = 0.4) + 
  scale_x_date("Date", date_breaks = "1 week", date_labels = "%b %d") +
  #scale_x_discrete(limits=c("Before", "After")) +
  scale_y_continuous("Travel Time (min)", limits = c(0,15)) +
  guides(fill = guide_legend(title = NULL)) +
  facet_grid(. ~ time_bin2) +
  labs(title="College St Travel Time: Dovercourt - Bathurst") +
  #theme(legend.title=element_blank())+
  theme_fivethirtyeight() + 
  scale_colour_fivethirtyeight()

#stat_summary(fun.y=mean,geom="bar") +
#stat_summary(fun.y = mean, geom = "point",  size = 5) +
#stat_summary(fun.data = "myFunc",geom = "errorbar", width = 0.2) +
#geom_boxplot(alpha = 0.4) +

fun_mean <- function(x){
  return(data.frame(y=mean(x),label=mean(x,na.rm=T)))}

#geom_smooth() +

#geom_boxplot(aes(group = date)) +
  #facet_wrap(~ segment)
  #+theme_fivethirtyeight()


#add time bin
bt_data  %>%
  mutate(time_bin = as.numeric(format(timestamp, "%H"))*100 +trunc(as.numeric(format(timestamp, "%M"))/15)+1 ) %>%
  select(timestamp, time_bin)

format(as.Date(bt_data$timestamp,format="%b %d %Y"), "%d")

as.Date(bt_data$timestamp,format="%b %d %Y")

end_date <- as.POSIXct(strptime("2015-12-24", "%Y-%m-%d"))

#plot of distribution of 
no_BT <- bt_data %>%
  filter(DeviceClass == 0,
         outlierLevel == 1,
         is.na(DeviceType)) %>%
  mutate(bin_string = paste(sapply(strsplit(paste(rev(intToBits(ClassOfDevice))),""),`[[`,2),collapse=""))

bin_string <- paste(sapply(strsplit(paste(rev(intToBits(bt_data$ClassOfDevice))),""),`[[`,2),collapse="")

write_csv(no_BT, "empty_BT.csv")



bar_plot <- bt_data %>%
  group_by(ClassOfDevice) %>%
  summarise(count = n())
write_csv(bar_plot, "Unique_BT_Codes.csv")




ggplot(bt_data, aes(x = factor(DeviceType))) +
  geom_bar()



