# II3502 Lab 6: Spark Climate Data Analysis

This project implements a distributed Spark application for analyzing NOAA Global Surface Summary of the Day (GSOD) climate data using RDD-based transformations and aggregations. The application processes large-scale climate datasets to compute temperature trends, precipitation patterns, and extreme weather event statistics.

## Overview

The application performs comprehensive climate data analysis including:
- **Data Loading**: Robust CSV parsing with automatic header detection
- **Data Cleaning**: Validation and filtering of invalid/missing values
- **Transformations**: Date parsing, seasonal assignment, and extreme event detection
- **Aggregations**: Monthly, yearly, and seasonal climate metric calculations
- **Analysis**: Temperature trends, precipitation patterns, and extreme weather statistics
- **Results Export**: Organized output files with summary statistics

## System Requirements

### Common Requirements (All Platforms)
- **Python**: Version 3.8 or higher
- **Java**: Version 8 or 11 (required for Apache Spark)
- **uv**: Modern Python package and project manager
- **Apache Spark**: 3.5.x (automatically installed via PySpark)

### Platform-Specific Requirements

#### Linux
- Python 3.8+ (usually pre-installed)
- Java JDK 8 or 11: `sudo apt-get install openjdk-11-jdk` (Debian/Ubuntu)
- uv: Install via `curl -LsSf https://astral.sh/uv/install.sh | sh`

#### Windows
- Python 3.8+ from [python.org](https://www.python.org/downloads/)
- Java JDK 8 or 11 from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [Adoptium](https://adoptium.net/)
- uv: Install via PowerShell `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`
- **Note**: PySpark on Windows may require additional configuration. Docker is recommended for Windows users (see Docker section below).

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Joaquim-Keloglanian/II3502_Lab6.git
cd II3502_Lab6
```

### 2. Install Dependencies

#### Using uv (Recommended)

```bash
# Install all dependencies including PySpark
uv sync
```

#### Using pip (Alternative)

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Linux/macOS:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install dependencies
pip install pyspark
```

## Usage

### Running on Linux

1. **Ensure Java is installed and JAVA_HOME is set**:
   ```bash
   # Check Java installation
   java -version
   
   # Set JAVA_HOME if needed (add to ~/.bashrc for persistence)
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   ```

2. **Run the application with default paths**:
   ```bash
   uv run python -m ii3502_lab6.climate_analysis
   ```

3. **Run with custom input/output paths**:
   ```bash
   uv run python -m ii3502_lab6.climate_analysis \
     --input /path/to/data/ \
     --output /path/to/results/
   ```

4. **Run with specific CSV file**:
   ```bash
   uv run python -m ii3502_lab6.climate_analysis \
     --input data/station_2025.csv \
     --output results/
   ```

### Running on Windows

**⚠️ IMPORTANT: Windows users MUST use Docker due to PySpark/Hadoop native library incompatibilities.**

Native PySpark execution on Windows requires Hadoop native libraries (winutils.exe) which cause numerous compatibility issues. The Docker method provides a consistent Linux environment that eliminates these problems entirely.

#### Using Docker (Required for Windows)

Docker eliminates Windows-specific PySpark compatibility issues and provides a consistent execution environment.

1. **Install Docker Desktop for Windows** from [docker.com](https://www.docker.com/products/docker-desktop/)

2. **Build the Docker image**:
   ```powershell
   docker build -t ii3502-lab6 .
   ```

3. **Run the container with volume mounts**:
   ```bash
   # On Git Bash / WSL / Bash (recommended for Windows users using a Unix-like shell)
   docker run --rm -v "$(pwd)/src/main/resources:/app/src/main/resources" ii3502-lab6
   ```

   ```powershell
   # Using PowerShell
   docker run --rm -v "${PWD}\src\main\resources:/app/src\main\resources" ii3502-lab6
   ```

   **If you prefer Command Prompt:**
   ```cmd
   docker run --rm -v "%CD%\src\main\resources:/app/src/main/resources" ii3502-lab6
   ```

   - The application will load data from `src/main/resources/data/` and save results to `src/main/resources/output/` on the host.
   - Running these commands will write results directly to `src/main/resources/output/` (no additional `output_host` directory required).
   - You can run the command repeatedly; the output subfolders will be automatically cleaned and overwritten each time.

   Note: If you run the Bash variant but supply Windows-style backslashes (e.g. `-v "${PWD}\src\main\resources:/app/..."`), Docker may misinterpret the path and create a malformed directory such as `src/main/resources;C` on the host. To avoid this:
   - Use the Bash command (Git Bash/WSL): `docker run --rm -v "$(pwd)/src/main/resources:/app/src/main/resources" ii3502-lab6`
   - Or convert to a Windows path explicitly: `docker run --rm -v "$(cygpath -w $(pwd))/src/main/resources:/app/src/main/resources" ii3502-lab6`
   - In PowerShell, prefer using the `${PWD}` expansion or an explicit absolute path (e.g., `docker run --rm -v "${PWD}\src\main\resources:/app/src/main/resources" ii3502-lab6`).

   If you see a directory named `src/main/resources;C`, remove it with:
   ```bash
   rm -rf "src/main/resources;C"
   ```

   Tip: we include a helper script `run-docker-windows.sh` in the project root that handles path conversion for Windows shells (uses `cygpath` when available) and runs the container with the correct mount. Example:
   ```bash
   # Run with default settings
   ./run-docker-windows.sh

   # Run the analysis through the script with custom arguments
   ./run-docker-windows.sh uv run python -m ii3502_lab6.climate_analysis --output src/main/resources/output/
   ```

4. **Run with custom command-line arguments**:
  ```powershell
 docker run --rm -v "${PWD}\src\main\resources:/app/src/main_resources" ii3502-lab6 uv run python -m ii3502_lab6.climate_analysis --input custom/path/ --output custom/output/

# Run with volume mount
docker run --rm \
  -v "$(pwd)/src/main/resources:/app/src/main/resources" \
  ii3502-lab6

# Interactive shell for debugging
docker run -it --rm \
  -v "$(pwd)/src/main/resources:/app/src/main/resources" \
  ii3502-lab6 bash
```

### Command-Line Arguments

```bash
python -m ii3502_lab6.climate_analysis [OPTIONS]

Options:
  --input PATH   Input path for GSOD CSV files. Can be:
                 - Single file: data/station.csv
                 - Directory: data/ (processes all CSV files)
                 - Glob pattern: data/*.csv
                 Default: src/main/resources/data/

  --output PATH  Output directory for analysis results
                 Default: src/main/resources/output/

  -h, --help     Show help message and exit
```

### Examples

```bash
# Example 1: Run with defaults (data from src/main/resources/data/)
uv run python -m ii3502_lab6.climate_analysis

# Example 2: Analyze specific year's data
uv run python -m ii3502_lab6.climate_analysis --input data/2025/ --output results/2025/

# Example 3: Process single station file
uv run python -m ii3502_lab6.climate_analysis --input data/01001099999.csv --output results/station_01001099999/

# Example 4: Use glob pattern for specific region
uv run python -m ii3502_lab6.climate_analysis --input "data/0100*.csv" --output results/region_0100/
```

## Input Data

### Data Source
Download NOAA GSOD data from the [NOAA Global Summary of the Day](https://www.ncei.noaa.gov/data/global-summary-of-the-day/) archive.

### Data Format
The application expects CSV files with the following structure:
- **Required Fields**: STATION, DATE, TEMP, MAX, MIN, PRCP, WDSP, GUST, FRSHTT
- **Full Field List**: STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, TEMP, TEMP_ATTRIBUTES, DEWP, DEWP_ATTRIBUTES, SLP, SLP_ATTRIBUTES, STP, STP_ATTRIBUTES, VISIB, VISIB_ATTRIBUTES, WDSP, WDSP_ATTRIBUTES, MXSPD, GUST, MAX, MAX_ATTRIBUTES, MIN, MIN_ATTRIBUTES, PRCP, PRCP_ATTRIBUTES, SNDP, FRSHTT

### Data Quality Notes
- **Missing Values**: Represented as `999.9` or `9999.9` in the source data
- **Validation**: The application automatically filters out invalid/missing values
- **CSV Formats**: Handles both standard format (28 fields) and format with comma in station name (29 fields)

### Sample Data
Sample GSOD data files are included in `src/main/resources/data/`:
- `01001099999.csv` - JAN MAYEN NOR NAVY, Norway
- `01001499999.csv` - SORSTOKKEN, Norway  
- `01002099999.csv` - Additional Norwegian station

## Output

### Output Structure
The application creates the following directories in the specified output path:

#### 1. `monthly_avg_temp/`
Monthly average temperatures per station
```
Format: station,year,month,avg_temp
Example: 01001099999,2025,1,24.96
```

#### 2. `yearly_avg_temp/`
Yearly average temperatures per station
```
Format: station,year,avg_temp
Example: 01001099999,2025,28.45
```

#### 3. `seasonal_prcp/`
Seasonal precipitation averages
```
Format: station,year,season,avg_prcp
Example: 01001099999,2025,Winter,0.05
```

#### 4. `highest_max_temp/`
Top 10 stations with highest maximum daily temperatures
```
Format: station,max_temp
Example: 01001499999,72.5
```

#### 5. `extreme_events/`
Count of extreme weather events per station and type
```
Format: station,event_type,count
Example: 01001099999,Rain,15
Events: Fog, Rain, Snow, Hail, Thunder, Tornado
```

#### 6. `summary/`
Human-readable summary statistics
```
Example:
Hottest year: 2025 with avg temp 34.39
Wettest station: 01001499999 with total prcp 4399.56
Highest gust: 01001099999 with 58.50
```

### Output Notes
- Each directory contains **2 files**:
  - `_SUCCESS`: Empty marker file indicating successful completion
  - `part-00000`: Single consolidated file with all results (thanks to `coalesce(1)`)
- The application uses `coalesce(1)` to consolidate output into single files, avoiding empty partitions
- For very large datasets, you may want to increase the coalesce number (e.g., `coalesce(4)`) for better performance
- To read output: `cat output/summary/part-00000` or `cat output/*/part-*` to read all parts
- **If you re-run the application, all output subfolders will be automatically deleted and replaced with new results.**

## Testing

### Run Unit Tests
```bash
# Linux/macOS
uv run python -m unittest discover src/test/python/

# Windows
uv run python -m unittest discover src\test\python\
```

### Run Integration Tests
The integration tests validate the complete end-to-end workflow using actual data files.

**Linux/macOS:**
```bash
uv run python -m unittest src/test/integration/test_end_to_end.py
```

**Windows:**
Integration tests require native Hadoop libraries not available on Windows. Use Docker to run integration tests:
```powershell
# Build the test image
docker build -t ii3502-lab6 .

# Run integration tests in container
docker run --rm `
  -v "${PWD}\src:/app/src" `
  ii3502-lab6 `
  uv run python -m unittest src/test/integration/test_end_to_end.py
```

**Integration Test Coverage:**
- Complete data loading pipeline from CSV files
- Data cleaning and validation
- All aggregation operations (monthly, yearly, seasonal)
- Extreme event detection
- Summary statistics generation
- Output file creation and validation

### Run Specific Test
```bash
uv run python -m unittest src.test.python.test_climate_analysis
```

### Run All Tests
```bash
# Linux/macOS
uv run python -m unittest discover src/test/

# Windows
uv run python -m unittest discover src\test\
```

## Project Structure

```
.
├── src
│   ├── main
│   │   ├── python
│   │   │   ├── ii3502_lab6
│   │   │   │   ├── __pycache__
│   │   │   │   │   ├── __init__.cpython-313.pyc
│   │   │   │   │   └── climate_analysis.cpython-313.pyc
│   │   │   │   ├── __init__.py
│   │   │   │   └── climate_analysis.py
│   │   │   └── ii3502_lab6.egg-info
│   │   │       ├── PKG-INFO
│   │   │       ├── SOURCES.txt
│   │   │       ├── dependency_links.txt
│   │   │       ├── requires.txt
│   │   │       └── top_level.txt
│   │   └── resources
│   │       ├── data
│   │       │   ├── 01001099999.csv
│   │       │   ├── 01001499999.csv
│   │       │   └── 01002099999.csv
│   │       └── output
│   │           ├── extreme_events
│   │           │   ├── _SUCCESS
│   │           │   └── part-00000
│   │           ├── highest_max_temp
│   │           │   ├── _SUCCESS
│   │           │   └── part-00000
│   │           ├── monthly_avg_temp
│   │           │   ├── _SUCCESS
│   │           │   └── part-00000
│   │           ├── seasonal_prcp
│   │           │   ├── _SUCCESS
│   │           │   └── part-00000
│   │           ├── summary
│   │           │   ├── _SUCCESS
│   │           │   └── part-00000
│   │           └── yearly_avg_temp
│   │               ├── _SUCCESS
│   │               └── part-00000
│   └── test
│       ├── integration
│       │   └── test_end_to_end.py
│       └── python
│           ├── __pycache__
│           │   ├── test_climate_analysis.cpython-313.pyc
│           │   └── test_spark.cpython-313.pyc
│           ├── __init__.py
│           ├── test_climate_analysis.py
│           └── test_spark.py
├── AGENTS.md
├── DAP_lab6_spark_task.pdf
├── Dockerfile
├── README.md
├── pyproject.toml
└── uv.lock

19 directories, 37 files
```

## Development

### Code Quality Tools
The project uses Ruff for code formatting and linting:

```bash
# Format code
uv run ruff format

# Check for issues
uv run ruff check

# Auto-fix issues
uv run ruff check --fix
```

### Pre-Commit Workflow
Before committing code:
1. `uv run ruff format` - Format code
2. `uv run ruff check` - Check for linting issues
3. Run all tests
4. Fix any issues
5. Repeat until all checks pass

### Commit Message Format
Follow the format specified in `AGENTS.md`:
```
<type>(scope): <title>

<bullet point>
<bullet point>

Refs: #<branch-name>
```

## Troubleshooting

### Common Issues

#### Java Not Found
**Error**: `JAVA_HOME is not set`
**Solution**:
- Linux: `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`
- Windows: Set `JAVA_HOME` in System Environment Variables

#### Windows PySpark Issues
**Error**: Various Hadoop/Winutils errors on Windows
**Solution**: Use Docker (see Docker instructions above)

#### Memory Issues
**Error**: `java.lang.OutOfMemoryError`
**Solution**: Adjust Spark memory settings:
```python
conf.set("spark.driver.memory", "4g")
conf.set("spark.executor.memory", "4g")
```

#### Permission Denied (Docker)
**Error**: Cannot write to output directory
**Solution**: Ensure Docker has access to shared drives (Docker Desktop settings)

#### Empty Output
**Error**: No data in output files
**Solution**: Check input path exists and contains valid CSV files

### Getting Help
- Check the [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- Review [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- Open an issue on the [GitHub repository](https://github.com/Joaquim-Keloglanian/II3502_Lab6/issues)

## Performance Optimization

### For Large Datasets
- Increase Spark executor memory: `--executor-memory 8g`
- Use more partitions: `sc.textFile(path, minPartitions=100)`
- Enable compression: `conf.set("spark.rdd.compress", "true")`

### For Small Datasets
- Reduce overhead: `setMaster("local[2]")` instead of `local[*]`
- Decrease executor memory to free resources

## License

This project is developed for educational purposes as part of the II3502 course at ISEP.

## Authors

- **Joaquim Keloglanian** - [GitHub](https://github.com/Joaquim-Keloglanian)

## Acknowledgments

- NOAA National Centers for Environmental Information for providing GSOD data
- Apache Spark community for the excellent distributed computing framework
- ISEP Faculty for course guidance and support