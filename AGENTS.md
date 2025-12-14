# Git Commit Message Format

```
git commit -m "<type>(operational scope): <title>.

<bullet point>
<bullet point>
<bullet point>
.
.
.

Refs: #<local branch name>
```

# Pre-commit Actions

- Run uv run ruff format
- Run uv run ruff check
- Run all tests
- Fix code (not the tests, unless the tests are somehow flawed)
- Repeat until code is working

# Implementation Plan for Spark Climate Data Analysis

## Overview
Implement a Spark application using RDD-based transformations and aggregations to analyze NOAA GSOD climate data. The application will load, clean, transform, aggregate, and save results for climate analysis.

## File structure
```
.
|-- src
|   |-- main
|   |   |-- python
|   |   |   `-- ii3502_lab6.egg-info
|   |   |       |-- PKG-INFO
|   |   |       |-- SOURCES.txt
|   |   |       |-- dependency_links.txt
|   |   |       `-- top_level.txt
|   |   `-- resources
|   `-- test
|       |-- integration
|       `-- python
|-- AGENTS.md
|-- DAP_lab6_spark_task.pdf
|-- DAP_lab6_spark_task.txt
|-- README.md
|-- pyproject.toml
`-- uv.lock
```

## Project Structure
- Place main Spark code in `src/main/python/ii3502_lab6/`
- Use `src/main/resources/` for data files or configurations
- Tests in `src/test/python/`
- Integration tests in `src/test/integration/`

## Step 1: Data Loading
- Use `SparkContext.textFile()` to load GSOD CSV files into RDDs
- Handle one or more files for a year (e.g., 2025 data)
- Parse CSV lines, skipping header if present

## Step 2: Data Cleaning
- Extract fields: STATION, DATE, TEMP, MAX, MIN, PRCP, WDSP, GUST, FRSHTT
- Filter out records with invalid values (e.g., 9999.9)
- Convert strings to appropriate types: floats for numeric fields, dates to year/month/season
- Define helper functions for parsing and validation

## Step 3: Data Transformation
- Map cleaned data to key-value pairs for aggregation
- Assign seasons: Winter (Dec-Feb), Spring (Mar-May), Summer (Jun-Aug), Autumn (Sep-Nov)
- Extract extreme event flags from FRSHTT (binary string for Fog, Rain, Snow, Hail, Thunder, Tornado)

## Step 4: Aggregations and Climate Analysis
- Compute monthly/yearly average temperatures per station
- Calculate long-term temperature trends (e.g., average change over years)
- Seasonal precipitation averages
- Find stations with highest max daily temperatures
- Detect extreme events: heat days, high-wind days, weather events from FRSHTT

## Step 5: Saving Results
- Use RDD actions like `saveAsTextFile()` to save aggregated results as CSV/TXT
- Include summary statistics: hottest year, wettest station, highest wind gust

## Optional Extensions
- Extension 1: Use station metadata to map to countries and aggregate at country level
- Extension 2: Implement parts using DataFrames/SQL for comparison

## Implementation Details
- Use PySpark for the application
- Handle large datasets efficiently with RDD operations
- Test with sample data first
- Document code with comments
- Follow pre-commit actions for code quality
