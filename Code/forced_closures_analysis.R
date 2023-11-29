
####packages####

if (!require("pacman")) install.packages("pacman")

pacman::p_load(devtools, dplyr, tidyverse, tidyr, stringr,  curl, gt, gtsummary, plyr)

library(odsR)

####data####

closures <- read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/closures_full.csv"))

api <- rbind(read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_1.csv")),
             read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_2.csv")),
             read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_3.csv")),
             read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/all_carehomes_all_info_full_Oct.csv")) %>% dplyr::mutate(odsCcgCode= NA, odsCcgName = NA))


df = merge(api, closures, by.x='locationId', by.y='Location.ID', all=T)

# 
# check = df[df['organisationType'].isnull()]
# 
# 
# check = df[(df['organisationType'].notnull())&(df['Closure Type'].notnull())]
# 
# 
# check = df[(df['organisationType'].isnull())&(df['Closure Type'].notnull())]
# 
# 


####Analysis####
#remove social care orgs - the NA is the...
#mising one from the api - 1-6777626312 - pain, it is a carehome though so will carry on not too worried




#df <- df %>% dplyr::filter(type!="Social Care Org",
#                           !is.na(type))




df %>% 
    dplyr::mutate(Closure.Type = ifelse(is.na(Closure.Type), "Never closed", Closure.Type))%>%
    dplyr::select(type, Closure.Type,inspectionDirectorate)%>%
    tbl_summary(
    by = c(Closure.Type), # split table by group
    #missing = "no",
    type = all_continuous() ~ "continuous2",
    statistic = all_continuous() ~ c("{N_nonmiss}",
                                     "{mean} ({median})", 
                                     "{min}, {max} ({sd})")# don't list missing data separately
  ) %>%
  add_n() %>% # add column with total number of non-missing observations
  #add_overall() %>%
  add_p() %>% # test for a difference between groups
  modify_spanning_header(c("stat_1", "stat_2") ~ "**Provider Sector**") %>%
  modify_caption("**Table. Provider Outcomes by Sector**") %>%
  modify_header(label = "**Variable**")%>%
  bold_labels()


er <- df %>% 
  dplyr::mutate(Closure.Type = ifelse(is.na(Closure.Type), "Never closed", Closure.Type))%>%
  dplyr::filter(type=="Independent Healthcare Org")%>%
  dplyr::mutate(Serves_NHS= ifelse(odsCode=="", "No", "Yes"))%>%
  dplyr::select(Serves_NHS, Closure.Type)%>%
  tbl_summary(
    by = c(Closure.Type), # split table by group
    #missing = "no",
    type = all_continuous() ~ "continuous2",
    statistic = all_continuous() ~ c("{N_nonmiss}",
                                     "{mean} ({median})", 
                                     "{min}, {max} ({sd})")# don't list missing data separately
  ) %>%
  add_n() %>% # add column with total number of non-missing observations
  #add_overall() %>%
  add_p() %>% # test for a difference between groups
  modify_spanning_header(c("stat_1", "stat_2") ~ "**Provider Sector**") %>%
  modify_caption("**Table. Provider Outcomes by Sector**") %>%
  modify_header(label = "**Variable**")%>%
  bold_labels()



er <- getODS(
  Name = "All",
  PostCode = "All",
  LastChangeDate = "All",
  Status = "Active",
  PrimaryRoleId = "RO176",
  NonPrimaryRoleId = "All",
  OrgRecordClass = "All",
  UseProxy = FALSE
)

er2 <- getODS(
  Name = "All",
  PostCode = "All",
  LastChangeDate = "All",
  Status = "Inactive",
  PrimaryRoleId = "RO176",
  NonPrimaryRoleId = "All",
  OrgRecordClass = "All",
  UseProxy = FALSE
)

# er3 <- getODS(
#   Name = "All",
#   PostCode = "All",
#   LastChangeDate = "All",
#   Status = "All",
#   PrimaryRoleId = "RO176",
#   NonPrimaryRoleId = "All",
#   OrgRecordClass = "All",
#   UseProxy = FALSE
# )


el <- as.data.frame(purrr::flatten(getODSfull(ODSCode = "A0A9J"))) 


yes <- as.data.frame(purrr::flatten(getODSfull(ODSCode = "A0A9J"))) %>%
  as.tibble() %>%
  unnest() %>%
  select(Name, Date.Start) %>%
  mutate(Date.End = NA)

skipped_values <- c()  # Vector to store skipped values

for (i in er$OrgId) {
  tryCatch({
    df <- as.data.frame(purrr::flatten(getODSfull(ODSCode = i))) %>%
      as.tibble() %>%
      unnest() %>%
      select(Name, Date.Start) %>%
      mutate(Date.End = NA)
    
    print(i)
    yes <- bind_rows(yes, df)
  }, error = function(e) {
    cat("Error occurred for ODSCode:", i, "- Skipping this iteration.\n")
    skipped_values <- c(skipped_values, i)  # Storing skipped value
  })
}

failed <- cat("Skipped ODSCodes:", skipped_values, "\n")



yes2 <- as.data.frame(purrr::flatten(getODSfull(ODSCode = "A5L8X"))) %>%
  as.tibble() %>%
  unnest() %>%
  select(Name, Date.Start, Date.End) 

skipped_values2 <- c()  # Vector to store skipped values

for (i in er2$OrgId) {
  tryCatch({
    df <- as.data.frame(purrr::flatten(getODSfull(ODSCode = i))) %>%
      as.tibble() %>%
      unnest() %>%
      select(Name, Date.Start, Date.End)
    
    print(i)
    yes2 <- bind_rows(yes2, df)
  }, error = function(e) {
    cat("Error occurred for ODSCode:", i, "- Skipping this iteration.\n")
    skipped_values2 <- c(skipped_values2, i)  # Storing skipped value
  })
}

failed2 <- cat("Skipped ODSCodes:", skipped_values2, "\n")


organizations <- unique(rbind(yes, yes2))





library(lubridate)

# Assuming 'Date.Start' and 'Date.End' are columns in your dataframe
# Convert Date.Start and Date.End columns to proper date format
organizations$Date.Start <- as.Date(organizations$Date.Start)
organizations$Date.End <- as.Date(organizations$Date.End)

# Get the minimum start date
min_start_date <- min(organizations$Date.Start, na.rm = TRUE)

# Create a sequence of months from the minimum start date to the current date
# Create a sequence of 6-month intervals from the minimum start date to the current date
all_months <- seq(min_start_date, Sys.Date(), by = "6 months")

# Initialize an empty vector to store counts
active_org_counts <- numeric(length(all_months))

# Loop through each 6-month interval
for (i in seq_along(all_months)) {
  current_month_start <- all_months[i]
  current_month_end <- current_month_start %m+% months(5) # Calculate 6 months interval
  
  active_org_counts[i] <- sum(
    organizations$Date.Start <= current_month_end &
      (is.na(organizations$Date.End) | organizations$Date.End >= current_month_start)
  )
}

# Create a dataframe with the counts for each 6-month interval
monthly_data <- data.frame(Interval_Start = all_months, Active_Organizations = active_org_counts)

ggplot(monthly_data, aes(x=all_months, y=Active_Organizations))+
  geom_point()+
  geom_smooth(span = 0.3)+
  theme_bw()+
  labs(x="Year", y="Number of Active Private Health Providers\nrelated to the NHS",
       title="The expansion of private companies 'interacting with' the NHS",
       caption = "Calculated based on the sites given ODS codes with primary role as: \n'INDEPENDENT SECTOR H/C PROVIDER SITE'")


