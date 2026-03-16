usethis::edit_git_ignore("project")

# download data!
if (!fs::file_exists("data.zip")) {
  curl::curl_download(
    "https://github.com/eribul/cs/raw/refs/heads/main/data.zip",
    "data.zip",
    quiet = FALSE
  )
}

library(tidyverse)
library(data.table)

# read without unzipping
patients <-
  readr::read_csv(unz("data.zip", "data-fixed/patients.csv")) |>
  setDT() |>
  setkey(id)

# remove empty rows/columns
patients <- janitor::remove_empty(patients, quiet = FALSE)
# A column with only one constant value is also not very interesting
patients <- janitor::remove_constant(patients, quiet = FALSE)


# Expectations/validations

library(pointblank)

# checking data! 6 different checks
checks <-
  patients |>
  create_agent(label = "A very simple example.") |>

  # date check (see label)
  col_vals_between(
    where(is.Date),
    as.Date("1900-01-01"),
    as.Date(Sys.Date()),
    na_pass = TRUE,
    label = "check that all dates (birth and death) are between 1900 and today's date)"
  ) |>
  
  # death date check (see label)
  col_vals_gte(
    deathdate,
    vars(birthdate),
    na_pass = TRUE,
    label = "check that death date, if it exists, is greater than birth date"
  ) |>

  # ssn check (see label)
  col_vals_regex(
    ssn, # This variable was described in ECS1!
    "[0-9]{3}-[0-9]{2}-[0-9]{4}$",
    label = "check that this is a valid ssn  (correct format) ([3 digits]-[2 digits]-[3 digits])"
  ) |>
  
  # id check (see label)
  col_is_integer(
    id,
    label = "check that the id is an integer"
  ) |>
  
  # gender check (see label)
  col_vals_in_set(
    gender,
    c("M", "F", "X"),
    label = "check that gender is one of three recognized markers in the US") |>
  
  # ethnicity check (see label)
  col_vals_in_set(
    ethnicity,
    c("hispanic", "nonhispanic"),
    label = "check that ethnicity is one of two recognized in the US") |>
  
  # marital status check (see label)
  col_vals_in_set(
    marital,
    c("D", "M", "S", "W"),
    label = "check that marital status is one of [divorced, married, single, widowed]") |>
  interrogate()

checks



# what kind of marital status?
patients[, .N, marital] 

# factor marital status
patients[,
  marital := factor(
    marital,
    levels = c("S", "M", "D", "W"),
    labels = c("Single", "Married", "Divorced", "Widowed")
  )
]

# factor gender
patients[,
  gender := factor(
    gender,
    levels = c("M", "F", "X"),
    labels = c("Male", "Female", "Non-binary")
  )
]


# race & state have good labels, so simply factoring them
patients[,
  names(.SD) := lapply(.SD, as.factor),
  .SDcols = c("race", "state")
]

# how many combinations of gender/race/state?
patients[, .N, .(gender, race, state)][order(N)]

# lump together all races that are less than 10% of the data
forcats::fct_lump_prop(patients$race, prop = 0.1)

# how old are folks?
patients[, age := as.integer((as.IDate(Sys.Date()) - birthdate)) %/% 365.241]
patients[, hist(age)]

# how old are ALIVE folks?
patients[is.na(deathdate), hist(age)]

# get only the transactions
unzip("data.zip", files = "data-fixed/payer_transitions.csv")
lastdate <-
  duckplyr::read_csv_duckdb("data-fixed/payer_transitions.csv") |>
  summarise(lastdate = max(start_date)) |>
  collect() |>
  pluck("lastdate") |>
  as.Date()

# hold were folks at the time?
patients[, age := as.integer((lastdate - birthdate)) %/% 365.241]
patients[, hist(age)]

# what are they called?
patients[,
  names(.SD) := lapply(.SD, \(x) replace_na(x, "")),
  .SDcols = c("prefix", "middle")
]

patients


patients[,
  full_name := paste(
    prefix,
    first,
    middle,
    last,
    fifelse(!suffix %in% c("", NA), paste0(", ", suffix), "")
  )
]

patients[, full_name]

# remove trailing spaces
patients[, names(.SD) := lapply(.SD, trimws), .SDcols = is.character]

# remove duplicated spaces
patients[, full_name := stringr::str_replace(full_name, "  ", " ")]

# remove some columns
patients[, c("prefix", "first", "middle", "last", "suffix", "maiden") := NULL]

# transform license to whether patient HAS a license
patients[, driver := !is.na(drivers)][, drivers := NULL]

# represent where the patients are on a map
leaflet::leaflet(data = patients) |>
  leaflet::addTiles() |>
  leaflet::addMarkers(~lon, ~lat, label = ~full_name)

# All of the data is concentrated in just a few states! It is not representative of all of the uSA and definitely not the whole world.


# convert to parquets (for fun, apparently :-))
zip::unzip("data.zip")
fs::dir_create("data-parquet")
csv2parquet <- function(file) {
  new_file <-
    file |>
    stringr::str_replace("-fixed", "-parquet") |>
    stringr::str_replace(".csv", ".parquet")

  duckplyr::read_csv_duckdb(file) |>
    duckplyr::compute_parquet(new_file)
}
fs::dir_ls("data-fixed/") |>
  purrr::walk(csv2parquet, .progress = TRUE)
fs::dir_delete("data-fixed")

# get procedure data
procedures <- duckplyr::read_parquet_duckdb("data-parquet/procedures.parquet")

# filter out data we need
procedures <-
  procedures |>
  select(patient, reasoncode_icd10, start) |>
  filter(!is.na(reasoncode_icd10)) |>
  collect()

# use data.table
setDT(procedures, key = "patient")

# get only the year
procedures[, year := year(start)][, start := NULL]

# filter out adults
proc_n_adults <-
  procedures[
    patients[, .(id, birthdate = as.IDate(birthdate))],
    on = c(patient = "id")
  ] |>
  _[year - year(birthdate) >= 18L, .N, .(reasoncode_icd10, year)]


# add descriptions to the codes (but they are in swedish sadly)
cond_by_year <- setDT(decoder::icd10se)[
  proc_n_adults,
  on = c(key = "reasoncode_icd10")
]


# visualize top 5
top5 <- cond_by_year[, .(N = sum(N)), .(value)][order(-N)][1:5, value]
ggplot(cond_by_year[.(top5), on = "value"], aes(year, N, color = value)) +
  geom_line() +
  theme(legend.position = "bottom") +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_discrete(
    labels = function(x) str_wrap(x, width = 40)
  )

# I think the year 2025 was not finished when the data was extracted, which makes it LOOK like the numbers went down, but actually it's justthat not all the data is present.

# It's incredibly possible that they didn't used to have as consistent recording as we do now. 

# We would definitely need to standardize numbers to population size, health seeking behaviour (which is also influenced by how much the government or the WHO encourages people to seek medical care, for example), data accuracy (perhaps less accurate in earlier years).

# I think they get more treatment. For example gingivitis, there has been a huge push in dental health and specifically gum health in more recent years. People are more aware, and more likely to seek medical care.
