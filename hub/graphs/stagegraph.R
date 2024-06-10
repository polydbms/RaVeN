#!/usr/bin/Rscript

renv::load("/home/gereon/git/dima/sds-graphs")

library(STBenchiGraphs)
library(argparse)
library(dplyr)
library(ggplot2)
library(ggbreak)
library(RColorBrewer)
library(stringr)
library(purrr)
library(tidyr)


# parse arguments ---------------------------------------------------------

parser <- ArgumentParser()
parser$add_argument("-s", "--set_id", help = "Benchmark set id to plot", type = "integer")
parser$add_argument("-o", "--output", help = "Output file")
parser$add_argument("-i", "--interaction_params", help = "Parameter that changes, comma-separated")
args <- parser$parse_args()

# constants ---------------------------------------------------------------
c <- STBenchiGraphs::connect_stbenchi_db("/tmp/results.tmp.db")

stageorder_lev <-
  c(
    "preprocess_vector_NA" = "Preprocess Vector",
    "preprocess_raster_NA" = "Preprocess Raster",
    "ingestion_vector_pre" = "Ingest Vector, Prepare",
    "ingestion_vector_system" = "Ingest Vector, Load",
    "ingestion_raster_pre" = "Ingest Raster, Prepare",
    "ingestion_raster_system" = "Ingest Raster, Load",
    "ingestion_vector_full" = "Ingest Vector",
    "ingestion_raster_full" = "Ingest Raster",
    "execution__NA" = "Execute",
    "full_duration" = "Full Duration"

  ) |> rev()

stageorder_colors <-
  setNames(brewer.pal(
    n = length(stageorder_lev |> as.character()),
    name = "Paired"
  ),
           stageorder_lev |> as.character())

interaction_params <- args$interaction_params |>
  strsplit(",") |>
  unlist()

interaction_params |> print()

# load data ---------------------------------------------------------------

raw_data <- STBenchiGraphs::prepare_timings_data_comparison(c, args$set_id)

only_cold <- raw_data |>
  select(exec_rev) |>
  distinct() |>
  drop_na() |>
  length() == 1

agg_data <- raw_data |>
  filter(!(
    stage == "ingestion" &
      system == "postgis" &
      ingest_dur_type == "full"
  )) |>
  mutate(
    system = case_match(
      system,
      "postgis" ~ "PostGIS",
      "rasdaman" ~ "RasDaMan",
      "sedona" ~ "Sedona (native)",
      "sedona-vec" ~ "Sedona (Vectorized)",
      "beast" ~ "Beast"
    )
  ) |>
  replace_na(list(exec_rev = "")) |>
  filter(exec_rev != "cold" | only_cold) |>
  mutate(
    stage_comb = paste(stage, dataset, ingest_dur_type, sep = "_") |> factor(stageorder_lev |> names(), labels =
      stageorder_lev),
  ) |>
  summarise(
    duration = mean(duration),
    .by = c(
      stage_comb,
      !!!rlang::syms(interaction_params)
    )
  ) |> print()

graph <- agg_data |>
  ggplot(aes(x = interaction(!!!rlang::syms(interaction_params)), y = duration, fill = stage_comb)) +
  geom_col() +
  theme_classic() +
  scale_y_continuous(expand = expansion(add = c(0, 10))) +
  labs(y = "Latency (sec)",
       x = paste(interaction_params, collapse = ", "),
       fill = "Stages") +
  scale_fill_manual(values = stageorder_colors) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

graph |> ggsave(filename = args$output, units = "mm", width = 200, height = 200, dpi = 300)