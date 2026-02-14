# scrapes content from a single AAC accident report article

## libraries ----

library(rvest)
library(httr)
library(dplyr)
library(stringr)
library(readr)

## target article ----

article_url <- "https://publications.americanalpineclub.org/articles/13195000501"

## helper: scrape one article and return a single-row tibble ----

scrape_article <- function(url) {
  response <- GET(url, timeout(30))
  stopifnot(status_code(response) == 200)

  html <- content(response, as = "text", encoding = "UTF-8") %>%
    read_html()

  # article id from url
  article_id <- str_extract(url, "[0-9]+$")

  # title: <h2 class="title">
  title <- html %>%
    html_element("h2.title") %>%
    html_text2()

  # subtitle / location: <h5> (populated in newer articles, empty in older)
  subtitle <- html %>%
    html_element("h5") %>%
    html_text2()
  subtitle <- if (length(subtitle) == 0 || is.na(subtitle) || subtitle == "") NA_character_ else subtitle

  # metadata span: newer articles have Author, Climb Year, Publication Year;
  # older articles have only Publication Year — use regex to extract each safely
  span_text <- html %>%
    html_element("div#article-container span") %>%
    html_text2()

  author           <- str_extract(span_text, "(?<=Author: )[^.]+") %>% str_trim()
  climb_year       <- str_extract(span_text, "(?<=Climb Year: )\\d{4}") %>% as.integer()
  publication_year <- str_extract(span_text, "(?<=Publication Year: )\\d{4}") %>% as.integer()

  # body text: all <p> tags inside div#article, collapsed into one string
  paragraphs <- html %>%
    html_elements("div#article p") %>%
    html_text2()

  body_text <- paste(paragraphs, collapse = "\n\n")

  # accident report flag: "anam" appears in S3 asset URLs for accident reports
  # (older articles: anam in PDF href; newer articles: anam- prefix in img src)
  article_srcs <- html %>%
    html_elements("div#article img") %>%
    html_attr("src")

  # pdf url (may not be present on all articles)
  pdf_url <- html %>%
    html_element(".link-container a[href$='.pdf']") %>%
    html_attr("href")

  is_accident_report <- any(grepl("anam", c(article_srcs, pdf_url), ignore.case = TRUE), na.rm = TRUE)

  pdf_url <- if (length(pdf_url) == 0 || is.na(pdf_url)) NA_character_ else pdf_url

  tibble(
    article_id         = article_id,
    url                = url,
    is_accident_report = is_accident_report,
    title              = title,
    subtitle           = subtitle,
    author             = author,
    publication_year   = publication_year,
    climb_year         = climb_year,
    body_text          = body_text,
    pdf_url            = pdf_url
  )
}

## single-article validation ----

result <- scrape_article(article_url)

glimpse(result)
cat("\n--- IS ACCIDENT REPORT ---\n", result$is_accident_report)
cat("\n--- TITLE ---\n", result$title)
cat("\n--- SUBTITLE ---\n", result$subtitle)
cat("\n--- AUTHOR ---\n", result$author)
cat("\n--- PUBLICATION YEAR ---\n", result$publication_year)
cat("\n--- CLIMB YEAR ---\n", result$climb_year)
cat("\n--- PDF ---\n", result$pdf_url)
cat("\n--- BODY TEXT ---\n", result$body_text)



## bulk scrape all articles from article_urls.csv ----

input_file  <- "data/article_urls.csv"
output_file <- "data/article_text.csv"
delay_sec   <- 0.4

all_urls <- read_csv(input_file, show_col_types = FALSE) %>%
  pull(url) %>%
  unique()

# resume: skip urls already present in the output file
if (file.exists(output_file)) {
  done_urls <- read_csv(output_file, show_col_types = FALSE) %>%
    pull(url)
  urls_to_scrape <- setdiff(all_urls, done_urls)
  message("Resuming: ", length(done_urls), " done, ",
          length(urls_to_scrape), " remaining.")
} else {
  urls_to_scrape <- all_urls
  message("Starting fresh: ", length(urls_to_scrape), " articles to scrape.")
}

total <- length(urls_to_scrape)

for (i in seq_along(urls_to_scrape)) {
  url <- urls_to_scrape[[i]]
  message("(", i, "/", total, ") ", url)

  row <- tryCatch(
    scrape_article(url),
    error = function(e) {
      message("  ERROR: ", e$message)
      NULL
    }
  )

  if (!is.null(row)) {
    write_csv(row, output_file, append = file.exists(output_file))
  }

  Sys.sleep(delay_sec)
}

message("Done. Output saved to: ", output_file)
