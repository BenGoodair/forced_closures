
####packages####

if (!require("pacman")) install.packages("pacman")

pacman::p_load(devtools, dplyr, tidyverse, tidyr, stringr,  curl)

closures <- read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/closures_full.csv"))

api <- rbind(read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_1.csv")),
             read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_2.csv")),
             read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_3.csv")),
             read.csv(curl("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_1.csv")),)


####Analysis####



closures = pd.read_csv("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/closures_full.csv", encoding='ISO-8859-1')

column_types = {'odsCcgName': 'str', 'unpublishedReports': 'str'}  # Replace 'Column48Name' and 'Column50Name' with the actual column names

nonch = pd.concat([
  pd.read_csv("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_1.csv", encoding='ISO-8859-1', dtype=column_types),
  pd.read_csv("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_2.csv", encoding='ISO-8859-1', dtype=column_types),
  pd.read_csv("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/not_carehomes_3.csv", encoding='ISO-8859-1', dtype=column_types),
  pd.read_csv("https://raw.githubusercontent.com/BenGoodair/forced_closures/main/Data/all_carehomes_all_info_full_Oct.csv", encoding='ISO-8859-1', dtype=column_types)
], ignore_index=True)


df = pd.merge(nonch, closures, left_on='locationId', right_on='ï»¿Location ID', how='outer')

check = df[df['organisationType'].isnull()]


check = df[(df['organisationType'].notnull())&(df['Closure Type'].notnull())]


check = df[(df['organisationType'].isnull())&(df['Closure Type'].notnull())]


#mising one from the api - 1-6777626312 - pain, it is a carehome though so will carry on not too worried


