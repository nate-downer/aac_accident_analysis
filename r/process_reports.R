# processes accident report text through Claude API to extract structured fields

## libraries ----

library(httr2)
library(jsonlite)
library(dplyr)
library(readr)
library(stringr)
library(glue)

## helpers ----

`%||%` <- function(x, y) if (is.null(x)) y else x

## config ----

input_file  <- "data/article_text_20260214.csv"
output_file <- "data/article_extracted.csv"
model       <- "claude-haiku-4-5-20251001"
delay_sec   <- 0.6   # stay within API rate limits

api_key <- Sys.getenv("ANTHROPIC_API_KEY")
if (api_key == "") stop("ANTHROPIC_API_KEY environment variable is not set.")

## prompt ----

system_prompt <- "You are an expert analyst of mountaineering accident reports.
Extract structured information from the report and return it as a JSON object
with exactly these fields:

- accident_date: date of the accident in YYYY-MM-DD format, or null if unknown
- time_of_day: one of 'morning', 'afternoon', 'evening', 'night', 'unknown'
- location_country: country where the accident occurred
- location_state_region: state, province, or region (null if unknown)
- location_peak_area: specific mountain, peak, or climbing area name (null if unknown)
- route_name: name of the specific climbing route (null if not mentioned)
- risk_factors: array of strings describing risk factors that contribute to the accident; strings must be one of the following:
  - 'Piton/Ice Screw'
  - 'Ascent Illness': HAPE, HACE, AMS, or ascending too fast.
  - 'Crampon Issues': Any crampon difficulty — clearing balled snow, putting on/taking off, or misuse (e.g. glissading with crampons).
  - 'Glissading'
  - 'Ski-related': Only when skiing at time of accident; not applied when skis are off.
  - 'Poor Position'
  - 'Visibility': Dark, whiteout, or snowblind at time of accident (not during rescue). Includes being late in the day with diminishing light.
  - 'Severe Weather / Act of God': Includes lightning.
  - 'Natural Rockfall': Rockfall not caused by humans; excludes objects dislodged by climbing parties.
  - 'Wildlife'
  - 'Avalanche'
  - 'Poor Cond/Seasonal Risk'
  - 'Cornice / Snow Bridge Collapse'
  - 'Bergschrund'
  - 'Crevasse / Moat / Berschrund'
  - 'Icefall / Serac / Ice Avalanche'
  - 'Exposure'
  - 'Non-Ascent Illness'
  - 'Off-route': Straying from the intended route; excludes failure to follow ranger/guide directions.
  - 'Rushed'
  - 'Run Out'
  - 'Crowds'
  - 'Inadequate Food/Water'
  - 'No Helmet'
  - 'Late in Day'
  - 'Late Start'
  - 'Party Separated'
  - 'Ledge Fall': Injurious landing on a ledge; excludes breaking ledges (see Handhold/Foothold Broke) and incidental/fortuitous landings.
  - 'Gym / Artificial'
  - 'Gym Climber'
  - 'Fatigue'
  - 'Large Group'
  - 'Distracted'
  - 'Object Dropped/Dislodged': Objects dropped or dislodged by climbing parties; includes dropped rope and gear. Excludes natural rockfall.
  - 'Handhold/Foothold Broke'
  - 'Knot & Tie-in Error'
  - 'No Backup or End Knot'
  - 'Gear Broke'
  - 'Intoxicated'
  - 'Inadequate Equipment': Missing or insufficient clothing/gear; excludes helmet (has its own category).
  - 'Inadequate Protection / Pulled': No or insufficient protection placed.
  - 'Anchor Failure / Error': Errors building or failures of anchors; can co-occur with Rappel/Lowering Error.
  - 'Stranded / Lost / Overdue'
  - 'Belay Error'
  - 'Rappel Error'
  - 'Lowering Error'
  - 'Miscommunication'
  - 'Pendulum'
- climbing_style: array of strings describing the climbing activity at the point of the accident; strings must be one of the following:
  - 'Descent'
  - 'Roped'
  - 'Trad Climbing'
  - 'Sport'
  - 'Top-Rope'
  - 'Aid & Big Wall Climbing'
  - 'Unroped': Includes glissade and self-arrest incidents.
  - 'Solo': Includes self-belayed climbing.
  - 'Climbing Alone'
  - 'Bouldering'
  - 'Non-climbing'
  - 'Alpine/Mountaineering'
  - 'Ice Climbing'
- party_members: a nested object containing the following fields:
  - name: the climber's name
  - age: the climber's age as a number (null if unknown)
  - status: one of 'no injury', 'minor injury', 'serious injury', 'fatal injury', 'unknown'

Return only the JSON object, no other text."

## helper: call Claude and return parsed list ----

call_claude <- function(body_text) {
  resp <- request("https://api.anthropic.com/v1/messages") %>%
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01"
    ) %>%
    req_body_json(list(
      model      = model,
      max_tokens = 1024,
      system     = system_prompt,
      messages   = list(
        list(role = "user", content = body_text)
      )
    ), auto_unbox = TRUE) %>%
    req_error(is_error = \(r) FALSE) %>%  # handle errors manually
    req_perform()

  if (resp_status(resp) != 200) {
    stop("API error ", resp_status(resp), ": ", resp_body_string(resp))
  }

  raw_text <- resp %>%
    resp_body_json() %>%
    .[["content"]] %>%
    .[[1]] %>%
    .[["text"]] %>%
    str_remove_all("^```json\\s*|^```\\s*|\\s*```$")

  fromJSON(raw_text)
}

## helper: flatten extracted list to a single-row tibble ----

flatten_extraction <- function(extracted, article_id, url) {
  # arrays: collapse to pipe-delimited strings
  risk_factors   <- if (length(extracted$risk_factors) > 0)
    paste(extracted$risk_factors, collapse = " | ") else NA_character_

  climbing_style <- if (length(extracted$climbing_style) > 0)
    paste(extracted$climbing_style, collapse = " | ") else NA_character_

  # party_members: nested list of objects — store as JSON string
  party_members  <- if (!is.null(extracted$party_members) && length(extracted$party_members) > 0)
    toJSON(extracted$party_members, auto_unbox = TRUE) else NA_character_

  tibble(
    article_id            = article_id,
    url                   = url,
    accident_date         = extracted$accident_date         %||% NA_character_,
    time_of_day           = extracted$time_of_day           %||% NA_character_,
    location_country      = extracted$location_country      %||% NA_character_,
    location_state_region = extracted$location_state_region %||% NA_character_,
    location_peak_area    = extracted$location_peak_area    %||% NA_character_,
    route_name            = extracted$route_name            %||% NA_character_,
    risk_factors          = risk_factors,
    climbing_style        = climbing_style,
    party_members         = party_members,
    extraction_error      = NA_character_
  )
}

## load input, filter to accident reports only ----

articles_raw <- read_csv(input_file, show_col_types = FALSE) 

#articles_raw %>% 
#  filter(is_accident_report == TRUE, !is.na(body_text), body_text != "") %>%
#  filter(!grepl("know the ropes", tolower(title))) %>%
#  head(10) %>%
#  View()

articles <- articles_raw %>%
  filter(is_accident_report == TRUE, !is.na(body_text), body_text != "") %>%
  filter(!grepl("know the ropes", tolower(title))) %>%
  mutate(all_text = paste(title, subtitle, body_text, sep = " | "))


# validate that the call works

#test_text <- as.character(paste(articles[1,9]))
#test_result <- call_claude(test_text)


# resume: skip articles already in the output file
if (file.exists(output_file)) {
  done_ids       <- read_csv(output_file, show_col_types = FALSE) %>% pull(article_id)
  articles       <- articles %>% filter(!article_id %in% done_ids)
  message("Resuming: ", length(done_ids), " done, ", nrow(articles), " remaining.")
} else {
  message("Starting fresh: ", nrow(articles), " articles to process.")
}

## process articles ----

total <- nrow(articles)

for (i in seq_len(total)) {
  row <- articles[i, ]
  message("(", i, "/", total, ") ", row$article_id, " — ", row$title)

  result <- tryCatch({
    extracted <- call_claude(row$all_text)
    flatten_extraction(extracted, row$article_id, row$url)
  }, error = function(e) {
    message("  ERROR: ", e$message)
    tibble(
      article_id            = row$article_id,
      url                   = row$url,
      accident_date         = NA_character_,
      time_of_day           = NA_character_,
      location_country      = NA_character_,
      location_state_region = NA_character_,
      location_peak_area    = NA_character_,
      route_name            = NA_character_,
      risk_factors          = NA_character_,
      climbing_style        = NA_character_,
      party_members         = NA_character_,
      extraction_error      = e$message
    )
  })

  write_csv(result, output_file, append = file.exists(output_file))

  Sys.sleep(delay_sec)
}

message("Done. Output saved to: ", output_file)
