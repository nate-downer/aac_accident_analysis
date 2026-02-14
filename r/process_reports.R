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
- route_difficulty: grade of the climb, likely matches one of these styles:
    - '5.10a PG'
    - '5.4'
    - '5.9X'
    - '4th Class'
    - 'M4'
    - 'WI4'
    - 'C1'
    - 'A4'
    - '6b'
    - 'V12'
- immediate_cause: array of strings describing risk factors that directly caused the accident; strings must be one of the following:
    - 'Fall on Rock'
    - 'Fall on Ice'
    - 'Fall on Snow'
    - 'Falling Rock, Ice, Object'
    - 'Illness'
    - 'Stranded / Lost'
    - 'Avalanche'
    - 'Rappel Failure / Error'
    - 'Lowering Error'
    - 'Fall from Anchor'
    - 'Anchor Failure'
    - 'Exposure'
    - 'Glissade Error'
    - 'Protection Pulled Out'
    - 'Failure to Follow Route'
    - 'Fall into Crevasse / Moat'
    - 'Faulty use of Crampons'
    - 'Ascending too Fast'
    - 'Skiing'
    - 'Lightning'
    - 'Equipment Failure'
    - 'Unknown'
- objective_risk_factors: array of strings describing the environmental risk factors that contributed to the accident; strings must be one of the following:
    - 'Visibility': Dark, whiteout, or snowblind at time of accident (not during rescue). Includes diminishing light late in the day.
    - 'Severe Weather / Act of God': Includes lightning.
    - 'Natural Rockfall': Rockfall not caused by humans; excludes objects dislodged by climbing parties.
    - 'Wildlife'
    - 'Poor Cond/Seasonal Risk'
    - 'Cornice / Snow Bridge Collapse'
    - 'Crevasse / Moat / Bergschrund'
    - 'Icefall / Serac / Ice Avalanche'
    - 'Non-Ascent Illness'
    - 'Gym / Artificial'
    - 'Handhold/Foothold Broke'
    - 'Inadequate Protection Available': Route is difficult or impossible to protect.
- subjective_risk_factors: array of strings describing the gear and skill based risk factors that contributed to the accident; strings must be one of the following:
    - 'Piton/Ice Screw'
    - 'Crampon Issues': Crampon difficulty — balling snow, putting on/off, or misuse (e.g. glissading with crampons).
    - 'Poor Position'
    - 'Off-route': Straying from the intended route; excludes failure to follow ranger/guide directions.
    - 'Run Out'
    - 'Inadequate Food/Water'
    - 'No Helmet'
    - 'Late in Day'
    - 'Late Start'
    - 'Fatigue'
    - 'Object Dropped/Dislodged': Party-dislodged objects including rope and gear; excludes natural rockfall.
    - 'Knot & Tie-in Error'
    - 'No Backup or End Knot'
    - 'Gear Broke'
    - 'Inadequate Equipment': Missing or insufficient clothing/gear; excludes helmet (has its own category).
    - 'Inadequate Protection / Pulled': No or insufficient protection placed.
    - 'Anchor Failure / Error': Anchor building errors or failures; can co-occur with Rappel/Lowering Error.
    - 'Stranded / Lost / Overdue'
    - 'Belay Error'
    - 'Rappel Error'
    - 'Lowering Error'
    - 'Pendulum'
- social_risk_factors: array of strings describing the social and psychological risk factors that contributed to the accident; strings must be one of the following:
    - 'Rushed'
    - 'Crowds'
    - 'Party Separated'
    - 'Gym Climber'
    - 'Large Group'
    - 'Distracted'
    - 'Intoxicated'
    - 'Miscommunication'
    - 'Familiarity': Overconfidence in familiar terrain.
    - 'Acceptance': Desire for group acceptance led to increased risk tolerance.
    - 'Consistency': Overcommitment to a goal despite changing conditions.
    - 'Expert Halo': Less experienced members deferred to a leader, accepting more risk than they would alone.
    - 'Tracks/Scarcity': Perceived competition for first position or a closing window of opportunity.
    - 'Social Facilitation': False sense of safety from the presence of other groups on the route.
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
    - name: the climber's name (only include people involved in the incident)
    - age: the climber's age as a number (null if unknown)
    - party_status: one of 'solo', 'party_member', 'party_leader', 'unknown'
    - injury_level: one of 'no injury', 'minor injury', 'serious injury', 'fatal injury', 'unknown'

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
  immediate_cause <- if (length(extracted$immediate_cause) > 0)
    paste(extracted$immediate_cause, collapse = " | ") else NA_character_

  objective_risk_factors <- if (length(extracted$objective_risk_factors) > 0)
    paste(extracted$objective_risk_factors, collapse = " | ") else NA_character_

  subjective_risk_factors <- if (length(extracted$subjective_risk_factors) > 0)
    paste(extracted$subjective_risk_factors, collapse = " | ") else NA_character_

  social_risk_factors <- if (length(extracted$social_risk_factors) > 0)
    paste(extracted$social_risk_factors, collapse = " | ") else NA_character_

  climbing_style <- if (length(extracted$climbing_style) > 0)
    paste(extracted$climbing_style, collapse = " | ") else NA_character_

  # party_members: nested list of objects — store as JSON string
  party_members <- if (!is.null(extracted$party_members) && length(extracted$party_members) > 0)
    toJSON(extracted$party_members, auto_unbox = TRUE) else NA_character_

  tibble(
    article_id              = article_id,
    url                     = url,
    accident_date           = extracted$accident_date           %||% NA_character_,
    time_of_day             = extracted$time_of_day             %||% NA_character_,
    location_country        = extracted$location_country        %||% NA_character_,
    location_state_region   = extracted$location_state_region   %||% NA_character_,
    location_peak_area      = extracted$location_peak_area      %||% NA_character_,
    route_name              = extracted$route_name              %||% NA_character_,
    route_difficulty        = extracted$route_difficulty        %||% NA_character_,
    immediate_cause         = immediate_cause,
    objective_risk_factors  = objective_risk_factors,
    subjective_risk_factors = subjective_risk_factors,
    social_risk_factors     = social_risk_factors,
    climbing_style          = climbing_style,
    party_members           = party_members,
    extraction_error        = NA_character_
  )
}

## load input, filter to accident reports only ----

articles_raw <- read_csv(input_file, show_col_types = FALSE)

articles <- articles_raw %>%
  filter(is_accident_report == TRUE, !is.na(body_text), body_text != "") %>%
  filter(!grepl("know the ropes", tolower(title))) %>%
  mutate(all_text = paste(title, subtitle, body_text, sep = " | "))


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
      article_id              = row$article_id,
      url                     = row$url,
      accident_date           = NA_character_,
      time_of_day             = NA_character_,
      location_country        = NA_character_,
      location_state_region   = NA_character_,
      location_peak_area      = NA_character_,
      route_name              = NA_character_,
      route_difficulty        = NA_character_,
      immediate_cause         = NA_character_,
      objective_risk_factors  = NA_character_,
      subjective_risk_factors = NA_character_,
      social_risk_factors     = NA_character_,
      climbing_style          = NA_character_,
      party_members           = NA_character_,
      extraction_error        = e$message
    )
  })

  write_csv(result, output_file, append = file.exists(output_file))

  Sys.sleep(delay_sec)
}

message("Done. Output saved to: ", output_file)
