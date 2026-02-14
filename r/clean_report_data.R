# joins extracted fields onto article text and splits party members into own table

## libraries ----

library(dplyr)
library(readr)
library(purrr)
library(jsonlite)
library(stringr)

## config ----

text_file      <- "data/article_text_20260214.csv"
extracted_file <- "data/article_extracted.csv"
output_main    <- "data/accident_data.csv"
output_party   <- "data/party_members.csv"

## load inputs ----

article_text <- read_csv(text_file,      show_col_types = FALSE)
extracted    <- read_csv(extracted_file, show_col_types = FALSE)

## build main accident_data table ----

# join article metadata onto extracted rows; drop party_members (moved to own table)
accident_data <- extracted %>%
  select(-party_members) %>%
  left_join(
    article_text %>%
      select(article_id, is_accident_report, title, subtitle, author,
             publication_year, climb_year),
    by = "article_id"
  )

## derive grade columns ----

# yds_grade: bare numeric YDS grade (e.g. "5.10a PG" -> "5.10", "5.9X" -> "5.9")
# yds_grade_index: integer of the sub-grade for correct ordering (5.10 > 5.9)
accident_data <- accident_data %>%
  mutate(
    yds_grade       = str_extract(route_difficulty, "5\\.\\d+"),
    yds_grade_index = as.integer(str_remove(yds_grade, "5\\."))
  )

## build party_members table ----

parse_party_members <- function(article_id, json_str) {
  if (is.na(json_str) || json_str == "") return(NULL)
  tryCatch({
    members <- fromJSON(json_str, simplifyDataFrame = TRUE)
    if (nrow(members) == 0) return(NULL)
    members$article_id <- article_id
    members
  }, error = function(e) NULL)
}

party_members <- map2(
  extracted$article_id,
  extracted$party_members,
  parse_party_members
) %>%
  bind_rows() %>%
  select(article_id, everything())

## summarise party members onto main table ----

party_summary <- party_members %>%
  group_by(article_id) %>%
  summarise(
    party_size       = sum(party_status %in% c("party_member", "party_leader")),
    n_no_injury      = sum(injury_level == "no injury"),
    n_minor_injury   = sum(injury_level == "minor injury"),
    n_serious_injury = sum(injury_level == "serious injury"),
    n_fatal_injury   = sum(injury_level == "fatal injury")
  )

accident_data <- accident_data %>%
  left_join(party_summary, by = "article_id")


## write outputs ----

write_csv(accident_data, output_main)
write_csv(party_members, output_party)

