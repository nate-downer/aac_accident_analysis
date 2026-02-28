# script to run a multi-step extraction pipeline to analyze accident reports

################################################################################

#' Pipeline Structure
#' 
#' Step 1:
#' - Set globals
#' - Define API call functions
#' - Set up category defenitions
#' - Do additional data cleaning to filter out summary reports, obits, etc.
#' 
#' Note: steps 2 - 5 will be run in a loop that can itterate over n articles
#' 
#' Step 2:
#' - Perform inital API call using a lite LLM model
#' - Extract all data from each article
#' 
#' Step 3:
#' - Perform a second API call using a lite LLM model
#' - Only extract the difficult data points (people, risk factors, injury levels)
#'
#' Step 4:
#' - Analyze the differnces between the first two runs
#' - Flag any data points where there is disagreement between the two runs
#' - Calculate a % agreement score for each article
#' - Comple the results into a new propt for...
#' 
#' Step 5:
#' - Perform a third API call using a lite model
#' - Review the first two outputs and the article text to create a final answer
#' - Also return a 'confidence score' from 0 to 100 reflecting the confidence in the output
#' 
#' End of the primary loop
#' 
#' Step 6:
#' - Analyze the output of the third API call 
#' - Create a data frame that includes the results from all three runs
#' 
#' Step 7: 
#' - Identify the top n most confusing articles using the % agreement and confidence score
#' - Repeat step 5, but use all three outputs and a heavy duty model
#' - This will allow us to get a better result for the most confusing articles
#' 
#' Step 8:
#' - Final cleaning of the resulting outputs

################################################################################

## libraries ----

library(httr2)
library(jsonlite)
library(dplyr)
library(readr)
library(stringr)
library(glue)
library(tidyr)


## gloabls ----

# file structure
text_input_file <- "data/article_text_20260214.csv"
run_id <- format(Sys.time(), "run_%Y%m%d_%H%M%S")
output_folder_path <- file.path("data/pipeline_runs", run_id)

# run params
min_publish_year <- 2010
max_publish_year <- 2018
max_articles_in_run <- 1000

lite_model <- "claude-haiku-4-5-20251001"
heavy_model <- "claude-sonnet-4-5-20250929"

delay_sec   <- 0.6    # stay within API rate limits
max_retries <- 2      # retry if extraction returns all NAs
pct_extra_run <- 0.05 # what portion of articles get an extra run from the heavy model
debug_mode <- FALSE

initial_max_tokens <- 2048 * 2                            # starting max_tokens for API calls
max_max_tokens <- initial_max_tokens * (2 ^ max_retries)  # maximum max_tokens allowed (doubles on each retry)

# api key
api_key <- Sys.getenv("ANTHROPIC_API_KEY")
if (api_key == "") stop("ANTHROPIC_API_KEY environment variable is not set.")


## prompt components ----

system_prompt_header <- "You are an expert analyst of mountaineering accident reports.
Extract structured information from the report and return it as a JSON object
with exactly these fields:"

descriptive_fields <- "
- is_accident_report: true if the text is an accident report, false if it is a differnt type of article
- accident_date: date of the accident in YYYY-MM-DD format, or null if unknown
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
"

risk_factor_fields <- "
- time_of_day: time that the accident occured; one of 'morning', 'afternoon', 'evening', 'night', 'unknown'
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
"

party_member_fields <- "
- party_members: a nested object containing the following fields:
    - name: the climber's name (only include people involved in the incident)
    - age: the climber's age as a number (null if unknown)
    - experience_level: one of 'novice', 'intermediate', or 'expert' (null if unknown)
    - party_status: one of 'solo', 'main_party_member', 'main_party_leader', 'alternate_party_member', 'unknown'
    - injury_level: one of 'no injury', 'minor injury', 'serious injury', 'fatal injury', 'unknown'
"

rescue_type_fields <- "
- party_self_rescued: true if the climbers rescued themselves without external help, otherwise false
- helicopter_used: true if a helicopter was used in the rescue, otherwise false
- party_had_radios: true if the climbers had radios, otherwise false
- party_had_sat_comms: true if the climbers had a satalite communications (including a Garmin InReach), otherwise false
- party_had_cell_service: true if the climbers had cell service, otherwise false
"

call_1_prompt <- glue(
  "{system_prompt_header}
  {descriptive_fields}
  {risk_factor_fields}
  {party_member_fields}
  {rescue_type_fields}

  IMPORTANT: Return ONLY the JSON object with no markdown formatting, no code blocks, no explanation, and no additional text before or after the JSON."
)

call_2_prompt <- glue(
  "{system_prompt_header}
  {risk_factor_fields}
  {party_member_fields}
  {rescue_type_fields}

  IMPORTANT: Return ONLY the JSON object with no markdown formatting, no code blocks, no explanation, and no additional text before or after the JSON."
)

call_3_prompt <- glue(
  "You are an expert analyst of mountaineering accident reports.
  Compare the information from these two structured outputs to the information in the report that they are based on.
  Evaluate what the most accurate output is and return it as a JSON object with exactly these fields:
  {descriptive_fields}
  {risk_factor_fields}
  {party_member_fields}
  {rescue_type_fields}

  - output_confidence_level: integer from 0 - 100 reflecting how confident you are that the information in this output is correct; 100 is complete confidence

  IMPORTANT: Return ONLY the JSON object with no markdown formatting, no code blocks, no explanation, and no additional text before or after the JSON."
)

call_4_prompt <- glue(
  "You are an expert analyst of mountaineering accident reports.
  Compare the information from these three structured outputs to the information in the report that they are based on.
  Evaluate what the most accurate output is and return it as a JSON object with exactly these fields:
  {descriptive_fields}
  {risk_factor_fields}
  {party_member_fields}
  {rescue_type_fields}

  - output_confidence_level: integer from 0 - 100 reflecting how confident you are that the information in this output is correct; 100 is complete confidence

  IMPORTANT: Return ONLY the JSON object with no markdown formatting, no code blocks, no explanation, and no additional text before or after the JSON."
)


## helpers ----

`%||%` <- function(x, y) if (is.null(x)) y else x

#' Extract JSON from Claude's response, handling markdown and extra text
#' @param text Raw text from Claude API
#' @return Cleaned JSON string
extract_json <- function(text) {
  # Remove markdown code fences at start
  text <- str_remove(text, "^```json\\s*")
  text <- str_remove(text, "^```\\s*")

  # Remove closing backticks and everything after them
  text <- str_remove(text, "```.*$")

  # Remove any trailing text that starts with common analysis markers
  # (but be conservative - only remove if it appears after whitespace/newlines)
  text <- str_remove(text, "\\n\\s*\\*\\*[Aa]nalysis.*$")
  text <- str_remove(text, "\\n\\s*\\*\\*[Ee]xtraction.*$")

  # Trim whitespace
  text <- str_trim(text)

  return(text)
}

#' Parse JSON with better error messages
#' @param json_string JSON string to parse
#' @return Parsed list
parse_json_safely <- function(json_string) {
  # Handle NA or empty input
  if (is.na(json_string) || is.null(json_string) || nchar(json_string) == 0) {
    stop("Empty or NA response from API - check API key and network connection")
  }

  tryCatch({
    fromJSON(json_string)
  }, error = function(e) {
    # Show first 200 chars of the problematic JSON
    preview <- substr(json_string, 1, 200)
    last_chars <- substr(json_string, max(1, nchar(json_string) - 100), nchar(json_string))
    stop(glue("JSON parse error: {e$message}\n",
              "First 200 chars: {preview}\n",
              "Last 100 chars: {last_chars}"))
  })
}


## api call functions ----

#' Generic Claude API call
#' @param text The article text to analyze
#' @param system_prompt The system prompt to use
#' @param model_name The Claude model to use
#' @param max_tokens Maximum tokens for the response (default: initial_max_tokens)
#' @return Parsed JSON list
call_claude <- function(text, system_prompt, model_name, max_tokens = initial_max_tokens) {
  if (debug_mode) {
    cat("\n=== DEBUG: API Request ===\n")
    cat("Model:", model_name, "\n")
    cat("Max tokens:", max_tokens, "\n")
    cat("Text length:", nchar(text), "characters\n")
    cat("System prompt length:", nchar(system_prompt), "characters\n")
  }

  resp <- request("https://api.anthropic.com/v1/messages") %>%
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01"
    ) %>%
    req_body_json(list(
      model      = model_name,
      max_tokens = max_tokens,
      system     = system_prompt,
      messages   = list(
        list(role = "user", content = text)
      )
    ), auto_unbox = TRUE) %>%
    req_error(is_error = \(r) FALSE) %>%
    req_perform()

  if (debug_mode) {
    cat("Response status:", resp_status(resp), "\n")
  }

  if (resp_status(resp) != 200) {
    error_body <- resp_body_string(resp)
    if (debug_mode) {
      cat("=== DEBUG: API Error Response ===\n")
      cat(error_body, "\n")
      cat("=========================\n")
    }
    stop("API error ", resp_status(resp), ": ", error_body)
  }

  # Extract text - store body first, then access
  body_json <- resp_body_json(resp)

  if (debug_mode) {
    cat("Response structure:\n")
    cat("  - has 'content':", !is.null(body_json$content), "\n")
    if (!is.null(body_json$content)) {
      cat("  - content length:", length(body_json$content), "\n")
      if (length(body_json$content) > 0) {
        cat("  - content[[1]] has 'text':", !is.null(body_json$content[[1]]$text), "\n")
      }
    }
    cat("  - full response keys:", paste(names(body_json), collapse = ", "), "\n")
  }

  # Use direct $ accessor instead of [[]]
  raw_text <- body_json$content[[1]]$text

  if (debug_mode) {
    cat("Extracted text length:", if(is.null(raw_text)) "NULL" else nchar(raw_text), "\n")
    if (!is.null(raw_text) && nchar(raw_text) > 0) {
      cat("First 200 chars:", substr(raw_text, 1, 200), "\n")
    }
    cat("=========================\n\n")
  }

  # Check for empty response
  if (is.null(raw_text) || is.na(raw_text) || nchar(raw_text) == 0) {
    if (debug_mode) {
      cat("=== DEBUG: Empty Response ===\n")
      cat("Full response body:\n")
      print(body_json)
      cat("=========================\n")
    }
    stop("API returned empty response")
  }

  clean_json <- extract_json(raw_text)
  parse_json_safely(clean_json)
}

#' Claude API call with comparison of two prior extractions (for judge)
#' @param text The article text
#' @param extraction1 First extraction result (list)
#' @param extraction2 Second extraction result (list)
#' @param system_prompt The system prompt
#' @param model_name The Claude model to use
#' @param max_tokens Maximum tokens for the response (default: initial_max_tokens)
#' @return Parsed JSON list
call_claude_with_comparison <- function(text, extraction1, extraction2, system_prompt, model_name, max_tokens = initial_max_tokens) {
  # Format the prior extractions as JSON strings
  extraction1_json <- toJSON(extraction1, auto_unbox = TRUE, pretty = TRUE)
  extraction2_json <- toJSON(extraction2, auto_unbox = TRUE, pretty = TRUE)

  # Build the user prompt
  user_prompt <- glue(
    "ORIGINAL ARTICLE TEXT:
    {text}

    EXTRACTION 1:
    {extraction1_json}

    EXTRACTION 2:
    {extraction2_json}

    Please analyze both extractions and the original text, then provide your final answer."
  )

  if (debug_mode) {
    cat("\n=== DEBUG: Judge API Call ===\n")
    cat("Model:", model_name, "\n")
    cat("Prompt length:", nchar(user_prompt), "characters\n")
  }

  resp <- request("https://api.anthropic.com/v1/messages") %>%
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01"
    ) %>%
    req_body_json(list(
      model      = model_name,
      max_tokens = max_tokens,
      system     = system_prompt,
      messages   = list(
        list(role = "user", content = user_prompt)
      )
    ), auto_unbox = TRUE) %>%
    req_error(is_error = \(r) FALSE) %>%
    req_perform()

  if (debug_mode) {
    cat("Response status:", resp_status(resp), "\n")
  }

  if (resp_status(resp) != 200) {
    error_body <- resp_body_string(resp)
    if (debug_mode) {
      cat("=== DEBUG: API Error ===\n")
      cat(error_body, "\n")
      cat("======================\n")
    }
    stop("API error ", resp_status(resp), ": ", error_body)
  }

  # Extract text - store body first, then access
  body_json <- resp_body_json(resp)
  raw_text <- body_json$content[[1]]$text

  if (debug_mode) {
    cat("Response length:", if(is.null(raw_text)) "NULL" else nchar(raw_text), "\n")
    cat("======================\n\n")
  }

  # Check for empty response
  if (is.null(raw_text) || is.na(raw_text) || nchar(raw_text) == 0) {
    if (debug_mode) {
      cat("=== DEBUG: Empty response ===\n")
      print(body_json)
      cat("======================\n")
    }
    stop("API returned empty response")
  }

  clean_json <- extract_json(raw_text)
  parse_json_safely(clean_json)
}

#' Claude API call with three prior extractions (for heavy model re-processing)
#' @param text The article text
#' @param extraction1 First extraction result (list)
#' @param extraction2 Second extraction result (list)
#' @param extraction3 Third extraction result (list)
#' @param system_prompt The system prompt
#' @param model_name The Claude model to use
#' @param max_tokens Maximum tokens for the response (default: initial_max_tokens)
#' @return Parsed JSON list
call_claude_with_three_extractions <- function(text, extraction1, extraction2, extraction3, system_prompt, model_name, max_tokens = initial_max_tokens) {
  # Format the prior extractions as JSON strings
  extraction1_json <- toJSON(extraction1, auto_unbox = TRUE, pretty = TRUE)
  extraction2_json <- toJSON(extraction2, auto_unbox = TRUE, pretty = TRUE)
  extraction3_json <- toJSON(extraction3, auto_unbox = TRUE, pretty = TRUE)

  # Build the user prompt
  user_prompt <- glue(
    "ORIGINAL ARTICLE TEXT:
    {text}

    EXTRACTION 1:
    {extraction1_json}

    EXTRACTION 2:
    {extraction2_json}

    EXTRACTION 3 (JUDGED):
    {extraction3_json}

    Please analyze all three extractions and the original text, then provide your final answer."
  )

  if (debug_mode) {
    cat("\n=== DEBUG: Heavy Model API Call ===\n")
    cat("Model:", model_name, "\n")
    cat("Prompt length:", nchar(user_prompt), "characters\n")
  }

  resp <- request("https://api.anthropic.com/v1/messages") %>%
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01"
    ) %>%
    req_body_json(list(
      model      = model_name,
      max_tokens = max_tokens,
      system     = system_prompt,
      messages   = list(
        list(role = "user", content = user_prompt)
      )
    ), auto_unbox = TRUE) %>%
    req_error(is_error = \(r) FALSE) %>%
    req_perform()

  if (debug_mode) {
    cat("Response status:", resp_status(resp), "\n")
  }

  if (resp_status(resp) != 200) {
    error_body <- resp_body_string(resp)
    if (debug_mode) {
      cat("=== DEBUG: API Error ===\n")
      cat(error_body, "\n")
      cat("======================\n")
    }
    stop("API error ", resp_status(resp), ": ", error_body)
  }

  # Extract text - store body first, then access
  body_json <- resp_body_json(resp)
  raw_text <- body_json$content[[1]]$text

  if (debug_mode) {
    cat("Response length:", if(is.null(raw_text)) "NULL" else nchar(raw_text), "\n")
    cat("======================\n\n")
  }

  # Check for empty response
  if (is.null(raw_text) || is.na(raw_text) || nchar(raw_text) == 0) {
    if (debug_mode) {
      cat("=== DEBUG: Empty response ===\n")
      print(body_json)
      cat("======================\n")
    }
    stop("API returned empty response")
  }

  clean_json <- extract_json(raw_text)
  parse_json_safely(clean_json)
}

#' Flatten extraction to single-row tibble
#' @param extracted Parsed JSON list
#' @param article_id Article ID
#' @param url Article URL
#' @param source_step Which step produced this extraction
#' @return Single-row tibble
flatten_extraction <- function(extracted, article_id, url, source_step) {
  # Arrays: collapse to pipe-delimited strings
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

  # Party members: nested list of objects — store as JSON string
  party_members <- if (!is.null(extracted$party_members) && length(extracted$party_members) > 0)
    toJSON(extracted$party_members, auto_unbox = TRUE) else NA_character_

  tibble(
    article_id                = article_id,
    url                       = url,
    source_step               = source_step,
    is_accident_report        = extracted$is_accident_report        %||% NA,
    accident_date             = extracted$accident_date             %||% NA_character_,
    time_of_day               = extracted$time_of_day               %||% NA_character_,
    location_country          = extracted$location_country          %||% NA_character_,
    location_state_region     = extracted$location_state_region     %||% NA_character_,
    location_peak_area        = extracted$location_peak_area        %||% NA_character_,
    route_name                = extracted$route_name                %||% NA_character_,
    route_difficulty          = extracted$route_difficulty          %||% NA_character_,
    immediate_cause           = immediate_cause,
    objective_risk_factors    = objective_risk_factors,
    subjective_risk_factors   = subjective_risk_factors,
    social_risk_factors       = social_risk_factors,
    climbing_style            = climbing_style,
    party_members             = party_members,
    party_self_rescued        = extracted$party_self_rescued        %||% NA,
    helicopter_used           = extracted$helicopter_used           %||% NA,
    party_had_radios          = extracted$party_had_radios          %||% NA,
    party_had_sat_comms       = extracted$party_had_sat_comms       %||% NA,
    party_had_cell_service    = extracted$party_had_cell_service    %||% NA,
    output_confidence_level   = extracted$output_confidence_level   %||% NA_integer_,
    extraction_error          = NA_character_
  )
}

#' Calculate Jaccard similarity between two sets
#' @param set1 Pipe-delimited string or NA
#' @param set2 Pipe-delimited string or NA
#' @return Jaccard similarity (0-100)
jaccard_similarity <- function(set1, set2) {
  # Handle NA cases
  if (is.na(set1) && is.na(set2)) return(100)  # both empty = perfect match
  if (is.na(set1) || is.na(set2)) return(0)    # one empty, one not = no match

  # Split into sets
  s1 <- str_split(set1, " \\| ")[[1]]
  s2 <- str_split(set2, " \\| ")[[1]]

  # Calculate Jaccard
  intersection <- length(intersect(s1, s2))
  union <- length(union(s1, s2))

  if (union == 0) return(100)
  round(100 * intersection / union, 1)
}

#' Calculate agreement between two extractions
#' @param row1 Flattened extraction tibble row
#' @param row2 Flattened extraction tibble row
#' @return Tibble with agreement metrics
calculate_agreement <- function(row1, row2) {
  tibble(
    article_id = row1$article_id,
    immediate_cause_agreement       = jaccard_similarity(row1$immediate_cause, row2$immediate_cause),
    objective_risk_factors_agreement = jaccard_similarity(row1$objective_risk_factors, row2$objective_risk_factors),
    subjective_risk_factors_agreement = jaccard_similarity(row1$subjective_risk_factors, row2$subjective_risk_factors),
    social_risk_factors_agreement    = jaccard_similarity(row1$social_risk_factors, row2$social_risk_factors),
    climbing_style_agreement         = jaccard_similarity(row1$climbing_style, row2$climbing_style)
  ) %>%
    mutate(
      overall_agreement = round(
        (immediate_cause_agreement + objective_risk_factors_agreement +
         subjective_risk_factors_agreement + social_risk_factors_agreement +
         climbing_style_agreement) / 5, 1
      )
    )
}

#' Check if extraction failed
#' @param result Flattened extraction tibble row
#' @return TRUE if key fields are all NA
is_empty_result <- function(result) {
  # Handle case where result doesn't have expected columns (error tibble)
  if (!("time_of_day" %in% names(result))) return(FALSE)

  is.na(result$time_of_day) &&
  is.na(result$location_country) &&
  is.na(result$immediate_cause)
}

#' Create an error result tibble with all required columns
#' @param article_id Article ID
#' @param url Article URL
#' @param source_step Source step name
#' @param error_msg Error message
#' @return Tibble with all columns filled with NAs except error info
create_error_result <- function(article_id, url, source_step, error_msg) {
  tibble(
    article_id                = article_id,
    url                       = url,
    source_step               = source_step,
    is_accident_report        = NA,
    accident_date             = NA_character_,
    time_of_day               = NA_character_,
    location_country          = NA_character_,
    location_state_region     = NA_character_,
    location_peak_area        = NA_character_,
    route_name                = NA_character_,
    route_difficulty          = NA_character_,
    immediate_cause           = NA_character_,
    objective_risk_factors    = NA_character_,
    subjective_risk_factors   = NA_character_,
    social_risk_factors       = NA_character_,
    climbing_style            = NA_character_,
    party_members             = NA_character_,
    party_self_rescued        = NA,
    helicopter_used           = NA,
    party_had_radios          = NA,
    party_had_sat_comms       = NA,
    party_had_cell_service    = NA,
    output_confidence_level   = NA_integer_,
    extraction_error          = error_msg
  )
}


## additional data cleaning ----

# Create output folder
dir.create(output_folder_path, recursive = TRUE, showWarnings = FALSE)
message("Run ID: ", run_id)
message("Output folder: ", output_folder_path)

articles_raw <- read_csv(text_input_file, show_col_types = FALSE)

exclude_pattern <- "know the ropes|summary|in memoriam|members who passed"

articles <- articles_raw %>%
  mutate(
    article_type = case_when(
      is_accident_report == TRUE ~ "Accident Report",
      grepl('accident|injury', body_text) ~ "Other - Accident Mention",
      TRUE ~ "Other - No Accident",
    )
  ) %>%
  filter(article_type %in% c("Accident Report", "Other - Accident Mention")) %>%
  filter(!grepl(exclude_pattern, tolower(title))) %>%
  mutate(
    all_text = paste(
      title,
      subtitle,
      if_else(nchar(author) > 5, glue("Author: {author}"), ""),
      if_else(!is.na(publication_year), glue("Publication Year: {publication_year}"), ""),
      if_else(!is.na(climb_year), glue("Climb Year: {climb_year}"), ""),
      body_text,
      sep = "\n"
    )
  ) %>%
  filter(publication_year >= min_publish_year) %>% 
  filter(publication_year <= max_publish_year) %>% 
  arrange(title) %>%
  head(max_articles_in_run)


## run the main extraction loop ----

# Define output file paths
step2_file <- file.path(output_folder_path, "step2_extraction_1.csv")
step3_file <- file.path(output_folder_path, "step3_extraction_2.csv")
step4_file <- file.path(output_folder_path, "step4_comparison.csv")
step5_file <- file.path(output_folder_path, "step5_judged.csv")

# Initialize raw extraction storage (needed for Step 5)
raw_extractions_step2 <- list()
raw_extractions_step3 <- list()

total <- nrow(articles)
message("Processing ", total, " articles through steps 2-5...")
message("Results will be saved incrementally after each article.")

for (i in seq_len(total)) {
  row <- articles[i, ]
  message("\n(", i, "/", total, ") ", row$article_id, " — ", row$title)

  # Step 2: First extraction (all fields)
  message("  Step 2: First extraction...")
  attempt <- 0
  result_2 <- NULL
  raw_2 <- NULL
  current_max_tokens <- initial_max_tokens

  while (attempt <= max_retries) {
    if (attempt > 0) {
      message("    Retrying (attempt ", attempt, "/", max_retries, ") with max_tokens = ", current_max_tokens, "...")
      Sys.sleep(delay_sec * 2)
    }
    attempt <- attempt + 1

    result_2 <- tryCatch({
      raw_2 <- call_claude(row$all_text, call_1_prompt, lite_model, max_tokens = current_max_tokens)
      flatten_extraction(raw_2, row$article_id, row$url, "step2_extraction_1")
    }, error = function(e) {
      message("    ERROR: ", e$message)
      create_error_result(row$article_id, row$url, "step2_extraction_1", e$message)
    })

    if (!is_empty_result(result_2)) break
    # Double max_tokens for next retry, up to the maximum
    current_max_tokens <- min(current_max_tokens * 2, max_max_tokens)
  }

  # Save Step 2 result immediately
  raw_extractions_step2[[i]] <- raw_2
  write_csv(result_2, step2_file, append = file.exists(step2_file))
  Sys.sleep(delay_sec)

  # Check if article is flagged as not an accident report
  if (!is.na(result_2$is_accident_report) && result_2$is_accident_report == FALSE) {
    message("  → Article flagged as NOT an accident report - skipping steps 3-5")
    # Store NULL for raw extraction to maintain list indices
    raw_extractions_step3[[i]] <- NULL
    next  # Skip to next article
  }

  # Step 3: Second extraction (risk factors, party, rescue only)
  message("  Step 3: Second extraction...")
  attempt <- 0
  result_3 <- NULL
  raw_3 <- NULL
  current_max_tokens <- initial_max_tokens

  while (attempt <= max_retries) {
    if (attempt > 0) {
      message("    Retrying (attempt ", attempt, "/", max_retries, ") with max_tokens = ", current_max_tokens, "...")
      Sys.sleep(delay_sec * 2)
    }
    attempt <- attempt + 1

    result_3 <- tryCatch({
      raw_3 <- call_claude(row$all_text, call_2_prompt, lite_model, max_tokens = current_max_tokens)
      flatten_extraction(raw_3, row$article_id, row$url, "step3_extraction_2")
    }, error = function(e) {
      message("    ERROR: ", e$message)
      create_error_result(row$article_id, row$url, "step3_extraction_2", e$message)
    })

    if (!is_empty_result(result_3)) break
    # Double max_tokens for next retry, up to the maximum
    current_max_tokens <- min(current_max_tokens * 2, max_max_tokens)
  }

  # Save Step 3 result immediately
  raw_extractions_step3[[i]] <- raw_3
  write_csv(result_3, step3_file, append = file.exists(step3_file))
  Sys.sleep(delay_sec)

  # Step 4: Calculate agreement
  message("  Step 4: Calculating agreement...")
  agreement <- calculate_agreement(result_2, result_3)
  # Save Step 4 result immediately
  write_csv(agreement, step4_file, append = file.exists(step4_file))
  message("    Overall agreement: ", agreement$overall_agreement, "%")

  # Step 5: Judge call (using raw extractions from steps 2 and 3)
  message("  Step 5: Judge call...")
  attempt <- 0
  result_5 <- NULL
  current_max_tokens <- initial_max_tokens

  while (attempt <= max_retries) {
    if (attempt > 0) {
      message("    Retrying (attempt ", attempt, "/", max_retries, ") with max_tokens = ", current_max_tokens, "...")
      Sys.sleep(delay_sec * 2)
    }
    attempt <- attempt + 1

    result_5 <- tryCatch({
      extracted_judge <- call_claude_with_comparison(
        row$all_text, raw_2, raw_3, call_3_prompt, lite_model, max_tokens = current_max_tokens
      )
      flatten_extraction(extracted_judge, row$article_id, row$url, "step5_judged")
    }, error = function(e) {
      message("    ERROR: ", e$message)
      create_error_result(row$article_id, row$url, "step5_judged", e$message)
    })

    if (!is_empty_result(result_5)) break
    # Double max_tokens for next retry, up to the maximum
    current_max_tokens <- min(current_max_tokens * 2, max_max_tokens)
  }

  # Save Step 5 result immediately
  write_csv(result_5, step5_file, append = file.exists(step5_file))
  message("    Confidence score: ", result_5$output_confidence_level %||% "NA")
  Sys.sleep(delay_sec)
}

# Load saved results from CSV files
message("\nLoading saved results...")

df_step2 <- if (file.exists(step2_file)) {
  read_csv(step2_file, show_col_types = FALSE)
} else {
  tibble()
}

df_step3 <- if (file.exists(step3_file)) {
  read_csv(step3_file, show_col_types = FALSE)
} else {
  tibble()
}

df_step4 <- if (file.exists(step4_file)) {
  read_csv(step4_file, show_col_types = FALSE)
} else {
  tibble()
}

df_step5 <- if (file.exists(step5_file)) {
  read_csv(step5_file, show_col_types = FALSE)
} else {
  tibble()
}

# Count articles flagged as non-accident reports
non_accident_reports <- df_step2 %>%
  filter(!is.na(is_accident_report) & is_accident_report == FALSE)

if (nrow(non_accident_reports) > 0) {
  message("  Articles flagged as NOT accident reports: ", nrow(non_accident_reports))
  write_csv(non_accident_reports, file.path(output_folder_path, "filtered_non_accidents.csv"))
}

message("Steps 2-5 complete!")
message("  Accident reports processed: ", nrow(df_step5))
message("  Non-accident reports filtered: ", nrow(non_accident_reports))


## step 6: combine all runs ----

message("\nStep 6: Combining all extractions...")

df_combined <- bind_rows(
  df_step2,
  df_step3,
  df_step5
) %>%
  left_join(df_step4, by = "article_id")

write_csv(df_combined, file.path(output_folder_path, "step6_combined.csv"))
message("Combined data saved.")


## step 7: re-process confusing articles with heavy model ----

message("\nStep 7: Identifying articles for heavy model re-processing...")

# Check if there are any accident reports to re-process
if (nrow(df_step5) == 0) {
  message("  No accident reports to re-process - all were filtered.")
  df_step7 <- tibble()
} else {
  # Calculate confusion score for each article and select top N% most confusing
  # Confusion score = (100 - confidence) + (100 - agreement)
  # Higher score = more confusing
  confusing_articles <- df_step5 %>%
    left_join(df_step4, by = "article_id") %>%
    mutate(
      # Handle NAs: if confidence or agreement is NA, treat as very confusing
      conf_score = if_else(is.na(output_confidence_level), 100, 100 - output_confidence_level),
      agr_score = if_else(is.na(overall_agreement), 100, 100 - overall_agreement),
      confusion_score = conf_score + agr_score
    ) %>%
    arrange(desc(confusion_score)) %>%
    select(article_id, output_confidence_level, overall_agreement, confusion_score)

  # Calculate how many articles to re-process (top N%)
  n_to_reprocess <- max(1, ceiling(nrow(confusing_articles) * pct_extra_run))
  confusing_articles <- confusing_articles %>% head(n_to_reprocess)

  message("  Total articles processed: ", nrow(df_step5))
  message("  Percentage for heavy model: ", round(pct_extra_run * 100, 1), "%")
  message("  Articles selected for re-processing: ", nrow(confusing_articles))

  if (nrow(confusing_articles) == 0) {
    message("  No articles need re-processing based on confusion scores.")
    df_step7 <- tibble()
  } else {
    message("  Re-processing ", nrow(confusing_articles), " confusing articles with ", heavy_model, "...")

    results_step7 <- list()

    for (i in seq_len(nrow(confusing_articles))) {
      confusing_id <- confusing_articles$article_id[i]
      message("\n    (", i, "/", nrow(confusing_articles), ") ", confusing_id)
      message("      Confidence: ", confusing_articles$output_confidence_level[i],
              " | Agreement: ", confusing_articles$overall_agreement[i], "%",
              " | Confusion Score: ", round(confusing_articles$confusion_score[i], 1))

      # Get original article text
      article_row <- articles %>% filter(article_id == confusing_id)

      # Get all three prior extractions (need raw lists for API call)
      attempt <- 0
      result_7 <- NULL
      current_max_tokens <- initial_max_tokens

      while (attempt <= max_retries) {
        if (attempt > 0) {
          message("        Retrying (attempt ", attempt, "/", max_retries, ") with max_tokens = ", current_max_tokens, "...")
          Sys.sleep(delay_sec * 2)
        }
        attempt <- attempt + 1

        result_7 <- tryCatch({
          # Re-extract with lite model to get raw lists
          extracted_1 <- call_claude(article_row$all_text, call_1_prompt, lite_model, max_tokens = current_max_tokens)
          extracted_2 <- call_claude(article_row$all_text, call_2_prompt, lite_model, max_tokens = current_max_tokens)
          extracted_3 <- call_claude_with_comparison(
            article_row$all_text, extracted_1, extracted_2, call_3_prompt, lite_model, max_tokens = current_max_tokens
          )

          # Now call heavy model with all three
          extracted_heavy <- call_claude_with_three_extractions(
            article_row$all_text, extracted_1, extracted_2, extracted_3,
            call_4_prompt, heavy_model, max_tokens = current_max_tokens
          )
          flatten_extraction(extracted_heavy, confusing_id, article_row$url, "step7_heavy_reprocess")
        }, error = function(e) {
          message("        ERROR: ", e$message)
          create_error_result(confusing_id, article_row$url, "step7_heavy_reprocess", e$message)
        })

        if (!is_empty_result(result_7)) break
        # Double max_tokens for next retry, up to the maximum
        current_max_tokens <- min(current_max_tokens * 2, max_max_tokens)
      }

      results_step7[[i]] <- result_7
      message("        Heavy model confidence: ", result_7$output_confidence_level %||% "NA")
      Sys.sleep(delay_sec)
    }

    df_step7 <- bind_rows(results_step7)
    write_csv(df_step7, file.path(output_folder_path, "step7_heavy_reprocess.csv"))
    message("  Heavy model re-processing complete!")
  }
}


## step 8: final output ----

message("\nStep 8: Creating final output...")

# Start with step 5 results (lite model judge)
final_extractions <- df_step5 %>%
  select(-source_step)

# Replace confusing articles with heavy model results
if (nrow(df_step7) > 0) {
  final_extractions <- final_extractions %>%
    filter(!article_id %in% df_step7$article_id) %>%
    bind_rows(
      df_step7 %>% select(-source_step)
    ) %>%
    arrange(article_id)
}

write_csv(final_extractions, file.path(output_folder_path, "final_extractions.csv"))

message("\n✓ Pipeline complete!")
message("  Total articles in input: ", total)
message("  Non-accident reports filtered: ", nrow(non_accident_reports),
        " (", round(100 * nrow(non_accident_reports) / total, 1), "%)")
message("  Accident reports processed: ", nrow(final_extractions),
        " (", round(100 * nrow(final_extractions) / total, 1), "%)")

if (nrow(final_extractions) > 0) {
  message("  Heavy model percentage: ", round(pct_extra_run * 100, 1), "%")
  message("  Articles re-processed with heavy model: ", nrow(df_step7),
          " (", round(100 * nrow(df_step7) / nrow(final_extractions), 1), "% of accident reports)")
} else {
  message("  ⚠ No accident reports to process - all articles were filtered")
}

message("  Final output: ", file.path(output_folder_path, "final_extractions.csv"))
message("  All intermediate files saved to: ", output_folder_path)
