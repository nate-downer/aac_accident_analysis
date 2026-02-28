# converts baseline data from Excel to CSV format

## libraries ----

library(readxl)
library(readr)
library(dplyr)

## config ----

input_file  <- "data/baseline_archive/_github-AAC_accidents_tagged_data.xlsx"
output_file <- "data/baseline_archive/baseline_data.csv"
sheet_name  <- 1  # read sheet 1

## read Excel file ----

baseline_data <- read_excel(input_file, sheet = sheet_name)

## write to CSV ----

write_csv(baseline_data, output_file)


## explore baseline data ----

test_factors <- c(
  'Fall on Rock',
  'Avalanche',
  'Large Group',
  'Visibility',
  'No Helmet',
  'Late in Day'
)

clean_baseline_date <- baseline_data %>%
  filter(`Publication Year` >= 2010) %>%
  mutate(
    `Fall on Rock` = grepl("Fall on Rock", `Accident Title`, ignore.case = TRUE),
    Avalanche      = !is.na(Avalanche),
    `Large Group`  = !is.na(`Large Group`),
    Visibility     = !is.na(Visibility),
    `No Helmet`    = !is.na(`No Helmet`),
    `Late in Day`  = !is.na(`Late in Day`)
  ) %>%
  select(
    ID,
    `Accident Title`,
    `Publication Year`,
    all_of(test_factors)
  )

## write cleaned baseline data ----

output_baseline <- "data/baseline_archive/clean_baseline_date.csv"
write_csv(clean_baseline_date, output_baseline)


