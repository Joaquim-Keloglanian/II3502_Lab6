# II3502 Lab 6: Spark Climate Data Analysis

This project implements a Spark application for analyzing NOAA Global Surface Summary of the Day (GSOD) climate data using RDD-based transformations and aggregations.

## Overview

The application loads, cleans, transforms, and analyzes climate observations from GSOD datasets. It computes various climate metrics such as temperature trends, precipitation patterns, and extreme weather events.

## Requirements

- Python >= 3.8
- Java 8 or 11 (required for PySpark)
- uv (for dependency management)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Joaquim-Keloglanian/II3502_Lab6.git
   cd II3502_Lab6
   ```

2. Install dependencies using uv:
   ```bash
   uv sync
   ```

This will install PySpark and other dependencies.

## Usage

### Running the Application

The main script is `climate_analysis.py`. Run it with input and output paths:

```bash
uv run python -m ii3502_lab6.climate_analysis <input_path> <output_path>
```

- `<input_path>`: Path to GSOD CSV file(s) (e.g., `data/2025.csv` or `data/*` for multiple files)
- `<output_path>`: Directory to save aggregated results

Example:
```bash
uv run python -m ii3502_lab6.climate_analysis data/2025.csv output/
```

### Input Data

- Download GSOD data from [NOAA GSOD](https://www.ncei.noaa.gov/data/global-summary-of-the-day/)
- Expected CSV fields: STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, TEMP, TEMP_ATTRIBUTES, DEWP, DEWP_ATTRIBUTES, SLP, SLP_ATTRIBUTES, STP, STP_ATTRIBUTES, VISIB, VISIB_ATTRIBUTES, WDSP, WDSP_ATTRIBUTES, MXSPD, GUST, MAX, MAX_ATTRIBUTES, MIN, MIN_ATTRIBUTES, PRCP, PRCP_ATTRIBUTES, SNDP, FRSHTT
- Invalid values are marked as 9999.9

### Expected Output

The application generates several output files in the specified directory:

- `monthly_avg_temp/`: Monthly average temperatures per station (station,year,month,avg_temp)
- `yearly_avg_temp/`: Yearly average temperatures per station (station,year,avg_temp)
- `seasonal_prcp/`: Seasonal precipitation averages (station,year,season,avg_prcp)
- `highest_max_temp/`: Top 10 stations with highest max daily temperatures (station,max_temp)
- `extreme_events/`: Count of extreme events per station and type (station,event_type,count)
- `summary/`: Summary statistics (hottest year, wettest station, highest wind gust)

## Testing

Run unit tests:
```bash
uv run python -m unittest discover src/test/python/
```

## Project Structure

```
.
|-- src
|   |-- main
|   |   |-- python
|   |   |   `-- ii3502_lab6
|   |   |       |-- __init__.py
|   |   |       `-- climate_analysis.py
|   |   `-- resources
|   `-- test
|       |-- integration
|       `-- python
|           |-- __init__.py
|           `-- test_climate_analysis.py
|-- AGENTS.md
|-- pyproject.toml
|-- README.md
|-- uv.lock
```

## Development

Follow pre-commit actions:
1. `uv run ruff format`
2. `uv run ruff check`
3. Run all tests
4. Fix code if needed
5. Repeat until passing

Commit messages follow the format in `AGENTS.md`.

## Optional Extensions

- **Extension 1**: Country-level aggregations using station metadata
- **Extension 2**: Implement parts using Spark DataFrames/SQL for comparison

## License

This project is for educational purposes as part of II3502 Distributed Architectures and Programming course.