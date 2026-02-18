# AAC Accident Analysis

Analysis of mountaineering accident reports published by the [American Alpine Club (AAC)](https://publications.americanalpineclub.org/).

This project is inspired by work by Eliot Caroom. His [Natural Langauge Analysis](https://github.com/ecaroom/climbing-accidents/tree/master) of accident reports can be found on GitHub.

## Project Goals

- Build a structured dataset from publicly available AAC accident reports
- Use LLM-assisted extraction to tag risk factors, climbing style, injury severity, and location from unstructured article text
- Identify patterns in accident causes, injury severity, and contributing risk factors
- Explore how consistently risk factors can be extracted across repeated runs (inter-run agreement)
- Build a climber-facing visualization tool to make it easy to see the risk factors that are most applicable to certain styles of climbing
- Direct climbers to the reports and recommendations that are most relevant to them

> **Note:** The data is assumed to be incomplete. Not all accidents are reported to the AAC, not all reported accidents have publicly available articles, and the extraction process is imperfect. Injury totals in this analysis generally undercount totals reported in the AAC's annual *Accidents in North American Climbing* (ANAC).

---

## Exploratory Analysis



## Pipeline

1. **`scrape_report_links.R`** — Collects article URLs from the AAC publications index
2. **`scrape_articles.R`** — Downloads article text for each URL
3. **`extract_report_data.R`** — Sends each article to the Claude API (`claude-haiku-4-5`) with a structured prompt; outputs tagged fields including location, cause, risk factors, climbing style, and party member injuries
4. **`clean_report_data.R`** — Joins extraction output to article metadata, parses party member JSON, computes party size and injury counts, and extracts YDS grade

## Requirements

- R with packages: `tidyverse`, `httr2`, `jsonlite`, `rvest`, `DT`
- An `ANTHROPIC_API_KEY` environment variable set for the extraction step
