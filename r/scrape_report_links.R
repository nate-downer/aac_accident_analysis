# scrapes data from the AAC's website to compile a local data set

## libraries ----

library(rvest)
library(httr)
library(dplyr)
library(readr)

## config ----

base_url    <- "https://publications.americanalpineclub.org"
index_url   <- paste0(base_url, "/articles?tab=accidents&page=")
total_pages <- 1444
delay_sec   <- 1.5   # polite crawl delay between requests
output_file <- "data/article_urls.csv"

dir.create("data", showWarnings = FALSE)

## helper: scrape one index page and return article URLs ----

scrape_index_page <- function(page_num) {
  url <- paste0(index_url, page_num)

  response <- tryCatch(
    GET(url, timeout(30)),
    error = function(e) {
      message("  Error fetching page ", page_num, ": ", e$message)
      return(NULL)
    }
  )

  if (is.null(response) || status_code(response) != 200) {
    message("  Bad response on page ", page_num, " (status ",
            status_code(response), ")")
    return(NULL)
  }

  html <- content(response, as = "text", encoding = "UTF-8") %>%
    read_html()

  # article links all follow /articles/{numeric-id}
  links <- html %>%
    html_elements("a[href^='/articles/']") %>%
    html_attr("href") %>%
    unique()

  # keep only links that are purely /articles/{digits}
  links <- links[grepl("^/articles/[0-9]+$", links)]

  paste0(base_url, links)
}

## scrape all index pages ----

# resume from where we left off if a partial file exists
if (file.exists(output_file)) {
  existing   <- read_csv(output_file, show_col_types = FALSE)
  all_urls   <- existing$url
  start_page <- existing$page %>% max() + 1
  message("Resuming from page ", start_page, " (", length(all_urls),
          " URLs already collected)")
} else {
  all_urls   <- character(0)
  start_page <- 1
}

for (page in seq(start_page, total_pages)) {
  message("Scraping page ", page, " / ", total_pages)

  urls <- scrape_index_page(page)

  if (!is.null(urls) && length(urls) > 0) {
    new_rows <- tibble(page = page, url = urls)

    # append to file
    write_csv(new_rows, output_file,
              append = file.exists(output_file))

    all_urls <- c(all_urls, urls)
  }

  Sys.sleep(delay_sec)
}

message("Done. Collected ", length(all_urls), " article URLs.")
message("Saved to: ", output_file)


