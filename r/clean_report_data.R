# joins extracted fields onto article text and splits party members into own table

## libraries ----

library(dplyr)
library(readr)
library(purrr)
library(jsonlite)

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
             publication_year, climb_year, body_text, pdf_url),
    by = "article_id"
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

## write outputs ----

write_csv(accident_data, output_main)
write_csv(party_members, output_party)

message("accident_data:  ", nrow(accident_data), " rows -> ", output_main)
message("party_members:  ", nrow(party_members), " rows -> ", output_party)
