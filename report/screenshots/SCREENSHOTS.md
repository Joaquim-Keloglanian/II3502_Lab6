# Screenshot Instructions for Spark Climate Data Analysis Report

This document specifies exactly what terminal screenshots to capture for the Lab 6 report. Keep screenshots minimal and focused on critical execution stages.

## Required Screenshots (6 Total)

### Screenshot 1: Spark Initialization
**Filename**: `01_spark_initialization.png`

**What to capture**:
- Terminal output showing Spark Context startup
- Spark master URL (`local[*]`)
- Application name (`ClimateDataAnalysis`)
- Default parallelism value
- Shows the beginning of execution up through the "Spark Context" section header

**How to capture**:
```bash
# Run the application and capture from start
uv run python -m ii3502_lab6.climate_analysis

# Capture terminal from start until you see:
# ======================================================================
# STEP: Spark Context
# ======================================================================
#   Spark master: local[*]
#   App name: ClimateDataAnalysis
#   Default parallelism (approx): 16
```

**What this demonstrates**: Successful Spark environment initialization with local mode configuration.

**Note for Windows users**: The application automatically detects Windows and uses Python-based file loading (compatibility mode) instead of Spark's textFile() to avoid Hadoop native library issues.

---

### Screenshot 2: Data Loading and Cleaning
**Filename**: `02_data_loading_cleaning.png`

**What to capture**:
- Data Loading section showing:
  - Number of files found
  - File paths listed (e.g., "01001099999.csv", "01001499999.csv", etc.)
  - Windows compatibility mode message (if on Windows)
  - Lines loaded per file
  - Total lines loaded
  - Preview of first few CSV lines
  - Timing for raw data preview operation
- Data Cleaning section showing:
  - Detected CSV header starting with "STATION,DATE,..."
  - "Extracting fields and validating records..." message
  - Valid records count after cleaning with timing

**How to capture**:
```bash
# Continue from Screenshot 1, capture the output showing:
# ======================================================================
# STEP: Data Loading
# ======================================================================
#   Found 3 file(s):
#    - src/main/resources/data/01001099999.csv
#    - src/main/resources/data/01001499999.csv
#    - src/main/resources/data/01002099999.csv
#   Loading files using Python (Windows compatibility mode)
#     Loaded 366 lines from 01001099999.csv
#     Loaded 366 lines from 01001499999.csv
#     Loaded 366 lines from 01002099999.csv
#   Total lines loaded: 1098
#   Preview (first lines) (took X.XXs):
#      STATION,DATE,LATITUDE,LONGITUDE,...
#      "01001099999","2025-01-01",...
#      "01001099999","2025-01-02",...
# 
# ======================================================================
# STEP: Data Cleaning
# ======================================================================
#   Detected header: STATION,DATE,LATITUDE,LONGITUDE,ELEVATION,NAME,TEMP,...
#   Extracting fields and validating records...
#   Valid records after cleaning: 1095 (count took X.XXs)
```

**What this demonstrates**: 
- Successful data loading from multiple CSV files
- Windows compatibility mode detection (if applicable)
- Header detection and removal
- Data cleaning and validation process with performance metrics
- The application handles 3 Norwegian weather station files from 2025

---

### Screenshot 3: Aggregations and Analysis
**Filename**: `03_aggregations_analysis.png`

**What to capture**:
- Data Transformation section showing:
  - "Transforming records and parsing dates/events..." message
- Aggregations & Analysis section showing:
  - Description of operations being performed
  - Distinct stations count (should be 3 for sample data)
  - Computed hottest year with value and timing (e.g., "2025 -> 34.39")
  - Computed wettest station with ID and total precipitation with timing
  - Computed highest gust with station ID and wind speed with timing

**How to capture**:
```bash
# Continue capturing, showing:
# ======================================================================
# STEP: Data Transformation
# ======================================================================
#   Transforming records and parsing dates/events...
#
# ======================================================================
# STEP: Aggregations & Analysis
# ======================================================================
#   Computing monthly averages, yearly averages, seasonal precipitation, highest temps and extreme event counts...
#   Distinct stations in data: 3
#   Computed hottest year (took X.XXs): 2025 -> 34.39
#   Computed wettest station (took X.XXs): 01001499999 -> 4399.56
#   Computed highest gust (took X.XXs): 01001099999 -> 58.50
```

**What this demonstrates**: 
- Execution of RDD transformations (map, filter operations)
- Parsing of dates into year, month, and season components
- Extreme event detection from FRSHTT flags
- Multiple aggregation operations with performance metrics:
  - Monthly average temperatures per station
  - Yearly average temperatures per station
  - Seasonal precipitation calculations
  - Identification of temperature extremes
  - Wind gust analysis
  - Extreme weather event counting

---

### Screenshot 4: Results Saving and Completion
**Filename**: `04_results_saving_completion.png`

**What to capture**:
- Saving Results section showing:
  - Output path (e.g., "src/main/resources/output/")
  - Each output category being saved with individual messages:
    - "Saving monthly averages..."
    - "Saved monthly averages (took X.XXs)"
    - "Saving yearly averages..."
    - "Saved yearly averages (took X.XXs)"
    - "Saving seasonal precipitation averages..."
    - "Saved seasonal precipitation (took X.XXs)"
    - "Saving highest max temp list..."
    - "Saved highest max temp list (took X.XXs)"
    - "Saving extreme events counts..."
    - "Saved extreme events counts (took X.XXs)"
  - Final completion message: "Analysis complete! Results saved successfully."
  - Return to terminal prompt

**How to capture**:
```bash
# Final section of output showing:
# ======================================================================
# STEP: Saving Results
# ======================================================================
#   Saving results to: src/main/resources/output/
#   Saving monthly averages...
#   Saved monthly averages (took X.XXs)
#   Saving yearly averages...
#   Saved yearly averages (took X.XXs)
#   Saving seasonal precipitation averages...
#   Saved seasonal precipitation (took X.XXs)
#   Saving highest max temp list...
#   Saved highest max temp list (took X.XXs)
#   Saving extreme events counts...
#   Saved extreme events counts (took X.XXs)
#   Analysis complete! Results saved successfully.
# 
# $ ← back to terminal prompt
```

**What this demonstrates**: 
- Successful completion of all aggregation computations
- Export of results using RDD `saveAsTextFile()` action
- Use of `coalesce(1)` to create single consolidated output files per category
- Performance metrics for each save operation
- The application creates 6 output directories:
  - monthly_avg_temp (station, year, month, avg_temp)
  - yearly_avg_temp (station, year, avg_temp)
  - seasonal_prcp (station, year, season, avg_prcp)
  - highest_max_temp (top 10 stations by max temperature)
  - extreme_events (station, event_type, count)
  - summary (human-readable statistics)

---

### Screenshot 5: Output Verification
**Filename**: `05_output_verification.png`

**What to capture**:
- Terminal commands showing output directory structure with all 6 subdirectories
- Sample content from multiple output files demonstrating successful data export
- Each subdirectory should contain `_SUCCESS` marker and `part-00000` data file

**How to capture**:
```bash
# Run these commands and capture output:

# Show output directory structure (Windows PowerShell)
tree /F src\main\resources\output
# OR on Git Bash/Linux:
# ls -R src/main/resources/output/

# Display summary statistics
echo "=== Summary Statistics ==="
cat src/main/resources/output/summary/part-00000
# Expected output:
# Hottest year: 2025 with avg temp 34.39
# Wettest station: 01001499999 with total prcp 4399.56
# Highest gust: 01001099999 with 58.50

# Display first 5 lines of monthly averages
echo "=== Monthly Averages (first 5) ==="
head -5 src/main/resources/output/monthly_avg_temp/part-00000
# Expected format: station,year,month,avg_temp
# Example: 01001099999,2025,8,46.31818181818182

# Display first 5 lines of yearly averages
echo "=== Yearly Averages (first 5) ==="
head -5 src/main/resources/output/yearly_avg_temp/part-00000
# Expected format: station,year,avg_temp

# Display seasonal precipitation
echo "=== Seasonal Precipitation (first 5) ==="
head -5 src/main/resources/output/seasonal_prcp/part-00000
# Expected format: station,year,season,avg_prcp

# Display highest max temperatures
echo "=== Highest Max Temperatures ==="
cat src/main/resources/output/highest_max_temp/part-00000
# Expected format: station,max_temp (top 10 stations)

# Display extreme events
echo "=== Extreme Events (first 10) ==="
head -10 src/main/resources/output/extreme_events/part-00000
# Expected format: station,event_type,count
# Event types: Fog, Rain, Snow, Hail, Thunder, Tornado
```

**What this demonstrates**: 
- Verification that all 6 output directories were created successfully
- Each directory contains both `_SUCCESS` marker (indicating successful RDD write) and `part-00000` data file
- Data is properly formatted and contains expected values:
  - Monthly averages show station-year-month aggregations
  - Yearly averages show station-year aggregations
  - Seasonal precipitation shows station-year-season aggregations
  - Highest max temp shows top 10 stations ranked by maximum temperature
  - Extreme events shows counts of weather events (Fog, Rain, Snow, Hail, Thunder, Tornado) per station
  - Summary provides human-readable statistics for hottest year, wettest station, and highest wind gust
- Use of `coalesce(1)` resulted in single consolidated files (part-00000) instead of multiple partitions
- Data format is CSV-like for easy import into analysis tools

---

### Screenshot 6: Spark UI
**Filename**: `06_spark_ui.png`

**What to capture**:
- Browser window showing Spark UI at http://localhost:4040
- Jobs tab showing completed jobs with stages
- Job execution timeline and DAG visualization
- Demonstrates Spark's web interface for monitoring

**How to capture**:
```bash
# After application completes and displays:
# "Spark UI is available at: http://localhost:4040"
# "Press Enter to stop the application and close the UI..."

# Open browser and navigate to http://localhost:4040
# Capture the Jobs tab showing:
# - List of completed jobs
# - Job stages and tasks
# - Duration and status of each job
```

**What this demonstrates**:
- Spark's built-in monitoring and debugging UI
- Job execution timeline with stages and tasks
- DAG visualization showing RDD dependencies
- Performance metrics for distributed operations
- Demonstrates that Spark UI remains accessible after execution completes

**Note**: The `-it -p 4040:4040` Docker flags enable interactive terminal and expose Spark UI to host.

---

## Screenshot Tips

### Terminal Window Setup
- Use a clean terminal with high contrast (dark background, light text recommended)
- Ensure font size is readable (at least 12pt)
- Terminal width: at least 100 characters to avoid excessive line wrapping
- Use a clear, monospace font (e.g., Consolas, Monaco, Courier New)

### Capture Settings
- Save screenshots as PNG for best quality
- Crop to show only relevant terminal content (no excess desktop space)
- Ensure text is sharp and readable when zoomed to 100%
- Include terminal prompt/command where relevant for context

### Windows Users (Git Bash/PowerShell)
```bash
# Git Bash (recommended for cleaner output and Unix-like commands)
uv run python -m ii3502_lab6.climate_analysis

# PowerShell (if preferred)
uv run python -m ii3502_lab6.climate_analysis

# Note: The application automatically detects Windows and uses Python-based
# file loading (compatibility mode) to avoid Hadoop native library issues.
# You'll see: "Loading files using Python (Windows compatibility mode)"
```

### Linux/macOS Users
```bash
# Standard terminal execution
uv run python -m ii3502_lab6.climate_analysis
```

### Docker Users
If running via Docker, capture the container output:
```bash
# Build the image (if not already built)
docker build -t ii3502-lab6 .

# Run the container with volume mount
docker run --rm \
    -v "$(pwd)/src/main/resources:/app/src/main/resources" \
    ii3502-lab6

# Note: Docker execution shows the same output structure as native execution
# The application runs in a Linux container environment with proper Hadoop support

# Then for Screenshot 5 (output verification), run commands on host system:
# Windows (Git Bash):
ls -R src/main/resources/output/
cat src/main/resources/output/summary/part-00000

# Windows (PowerShell):
tree /F src\main\resources\output
Get-Content src\main\resources\output\summary\part-00000

# Linux/macOS:
ls -R src/main/resources/output/
cat src/main/resources/output/summary/part-00000
```

---

## Screenshot Locations

Save all screenshots in:
```
report/screenshots/
├── 01_spark_initialization.png
├── 02_data_loading_cleaning.png
├── 03_aggregations_analysis.png
├── 04_results_saving_completion.png
├── 05_output_verification.png
└── 06_spark_ui.png
```

---

## Quality Checklist

Before submitting screenshots, verify:
- [ ] All 6 screenshots captured (5 terminal + 1 browser UI)
- [ ] Text is readable and not blurry
- [ ] Terminal output shows complete sections (not cut off mid-line)
- [ ] Timing values are visible (demonstrates performance)
- [ ] Output file content is visible and valid
- [ ] Terminal prompt is visible where appropriate (shows command completion)
- [ ] No sensitive information visible (paths, usernames if unwanted)
- [ ] Screenshots are properly named according to specifications
- [ ] All screenshots are in PNG format

---

## Alternative: Single Continuous Capture

If preferred, you can capture one long terminal session and crop it into the 5 required sections. Ensure each section clearly shows the boundaries specified above.

---

## Understanding the Output

### RDD Operations Visible in Screenshots

The terminal output demonstrates the following Spark RDD operations:

**Data Loading Phase:**
- `sc.parallelize()` - Creating RDD from in-memory data (Windows compatibility mode)
- `sc.textFile()` - Alternative for non-Windows systems
- `take(n)` - Showing preview of first records

**Data Cleaning Phase:**
- `filter()` - Removing header row and invalid records
- `map()` - Extracting fields from CSV format using Python's csv.reader
- `count()` - Counting valid records after cleaning

**Data Transformation Phase:**
- `map()` - Converting records to structured format with parsed dates
- `filter()` - Removing records with invalid dates

**Aggregation Phase:**
- `map()` - Creating key-value pairs for grouping
- `reduceByKey()` - Aggregating values (sum, count, max)
- `mapValues()` - Computing averages from sums and counts
- `flatMap()` - Extracting multiple extreme events from single records
- `sortBy()` - Ranking stations by metrics
- `distinct()` - Counting unique stations
- `first()` - Getting top-ranked result
- `take(10)` - Getting top 10 results

**Saving Phase:**
- `coalesce(1)` - Consolidating partitions into single output file
- `saveAsTextFile()` - Writing results to disk
- `sc.parallelize()` - Creating summary RDD from computed statistics

### Performance Metrics

Each major operation shows timing in seconds (e.g., "took 0.25s"), demonstrating:
- RDD lazy evaluation (transformations don't execute until action called)
- Action execution time (count, first, take, saveAsTextFile)
- The efficiency of distributed processing even on small local datasets

### Data Format Examples

From the actual output files:

**Monthly Averages:**
```csv
station_id,year,month,avg_temperature
01001099999,2025,8,46.31818181818182
```

**Summary Statistics:**
```text
Hottest year: 2025 with avg temp 34.39
Wettest station: 01001499999 with total prcp 4399.56
Highest gust: 01001099999 with 58.50
```

**Extreme Events:**
```csv
station_id,event_type,count
01001099999,Rain,15
01001099999,Fog,23
```

---

## Notes

- These screenshots document **local execution** with PySpark in `local[*]` mode (uses all available CPU cores)
- Includes **both terminal output** (Screenshots 1-5) and **Spark UI web interface** (Screenshot 6)
- The 6 screenshots cover the complete pipeline: 
  1. Initialization → 2. Loading & Cleaning → 3. Transformation & Aggregation → 4. Saving → 5. Verification → 6. Spark UI
- Keep total screenshot count minimal (5) to maintain report conciseness
- Each screenshot should be referenced in the report with LaTeX figures:
  - `\ref{fig:spark_init}` for Screenshot 1
  - `\ref{fig:data_loading}` for Screenshot 2
  - `\ref{fig:aggregations}` for Screenshot 3
  - `\ref{fig:results_saving}` for Screenshot 4
  - `\ref{fig:output_verification}` for Screenshot 5
  - `\ref{fig:spark_ui}` for Screenshot 6
- Sample data includes 3 Norwegian weather stations from 2025:
  - 01001099999 (JAN MAYEN NOR NAVY)
  - 01001499999 (SORSTOKKEN)
  - 01002099999
- Expected valid records after cleaning: ~1095 records
- Application uses structured logging with 70-character separator lines for clear step visualization
