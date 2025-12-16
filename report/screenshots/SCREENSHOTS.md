# Screenshot Instructions for Spark Climate Data Analysis Report

This document specifies exactly what terminal screenshots to capture for the Lab 6 report. Keep screenshots minimal and focused on critical execution stages.

## Required Screenshots (5 Total)

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

---

### Screenshot 2: Data Loading and Cleaning
**Filename**: `02_data_loading_cleaning.png`

**What to capture**:
- Data Loading section showing:
  - Number of files found
  - File paths listed
  - Preview of first few CSV lines
  - Total raw lines loaded with timing
- Data Cleaning section showing:
  - Detected CSV header
  - Valid records after cleaning with count and timing

**How to capture**:
```bash
# Continue from Screenshot 1, capture the output showing:
# ======================================================================
# STEP: Data Loading
# ======================================================================
# [file listing and counts]
# 
# ======================================================================
# STEP: Data Cleaning
# ======================================================================
#   Detected header: STATION,DATE,...
#   Extracting fields and validating records...
#   Valid records after cleaning: XXX (count took X.XXs)
```

**What this demonstrates**: Successful data loading, header detection, and data cleaning/validation process.

---

### Screenshot 3: Aggregations and Analysis
**Filename**: `03_aggregations_analysis.png`

**What to capture**:
- Data Transformation section
- Aggregations & Analysis section showing:
  - Distinct stations count
  - Computed hottest year with value and timing
  - Computed wettest station with value and timing
  - Computed highest gust with value and timing

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
#   Computing monthly averages, yearly averages...
#   Distinct stations in data: 3
#   Computed hottest year (took X.XXs): 2025 -> XX.XX
#   Computed wettest station (took X.XXs): XXXXXX -> XXXX.XX
#   Computed highest gust (took X.XXs): XXXXXX -> XX.XX
```

**What this demonstrates**: Execution of RDD transformations and aggregation operations with performance metrics.

---

### Screenshot 4: Results Saving and Completion
**Filename**: `04_results_saving_completion.png`

**What to capture**:
- Saving Results section showing:
  - Output path
  - Each output category being saved (monthly averages, yearly averages, etc.)
  - Timing for each save operation
  - Final completion message
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
#   [... other saves ...]
#   Analysis complete! Results saved successfully.
# 
# $ ← back to terminal prompt
```

**What this demonstrates**: Successful completion of all aggregations and export of results to output files.

---

### Screenshot 5: Output Verification
**Filename**: `05_output_verification.png`

**What to capture**:
- Terminal commands listing output directory structure
- Sample content from at least 2 output files (e.g., summary and monthly_avg_temp)

**How to capture**:
```bash
# Run these commands and capture output:

# Show output directory structure
ls -R src/main/resources/output/

# Display summary statistics
echo "=== Summary Statistics ==="
cat src/main/resources/output/summary/part-00000

# Display first 5 lines of monthly averages
echo "=== Monthly Averages (first 5) ==="
head -5 src/main/resources/output/monthly_avg_temp/part-00000

# Display extreme events
echo "=== Extreme Events ==="
cat src/main/resources/output/extreme_events/part-00000
```

**What this demonstrates**: Verification that all output files were created successfully with valid data.

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
# Git Bash (recommended for cleaner output)
uv run python -m ii3502_lab6.climate_analysis

# PowerShell (if preferred)
uv run python -m ii3502_lab6.climate_analysis
```

### Linux/macOS Users
```bash
# Standard terminal execution
uv run python -m ii3502_lab6.climate_analysis
```

### Docker Users
If running via Docker, capture the container output:
```bash
docker run --rm \
    -v "$(pwd)/src/main/resources:/app/src/main/resources" \
    ii3502-lab6

# Then for Screenshot 5, run verification commands on host system:
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
└── 05_output_verification.png
```

---

## Quality Checklist

Before submitting screenshots, verify:
- [ ] All 5 screenshots captured
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

## Notes

- These screenshots document **local execution** with PySpark, not cluster/distributed mode
- Focus on **terminal output** showing Spark job execution, not UI/browser
- The 5 screenshots cover the complete pipeline: initialization → loading → processing → saving → verification
- Keep total screenshot count minimal (5) to maintain report conciseness
- Each screenshot should be referenced in the report (Figures \ref{fig:spark_init} through \ref{fig:output_verification})
