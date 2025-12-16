#!/usr/bin/env python3
"""
Spark Climate Data Analysis Application
========================================

This application analyzes NOAA Global Surface Summary of the Day (GSOD) climate data
using Apache Spark's RDD-based transformations and aggregations. It performs
comprehensive climate analysis including temperature trends, precipitation patterns,
and extreme weather event detection.

Features:
    - Data loading from GSOD CSV files with automatic header detection
    - Robust data cleaning and validation with handling for inconsistent CSV formats
    - Monthly, yearly, and seasonal climate metric calculations
    - Extreme weather event analysis (fog, rain, snow, hail, thunder, tornado)
    - Summary statistics generation (hottest year, wettest station, highest wind gust)
    - Distributed processing using Apache Spark RDDs

Author: Joaquim Keloglanian
Course: II3502 - Distributed Architectures and Programming
Institution: ISEP
Date: December 2025
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime
import os
import argparse
import csv
import shutil


def parse_date(date_str):
  """
  Parse date string to extract year, month, and season components.

  Seasons are defined as:
    - Winter: December, January, February (12, 1, 2)
    - Spring: March, April, May (3, 4, 5)
    - Summer: June, July, August (6, 7, 8)
    - Autumn: September, October, November (9, 10, 11)

  Args:
      date_str (str): Date string in ISO format (YYYY-MM-DD)

  Returns:
      tuple: (year, month, season) where:
          - year (int): Four-digit year
          - month (int): Month number (1-12)
          - season (str): Season name ('Winter', 'Spring', 'Summer', 'Autumn')
          Returns (None, None, None) if date parsing fails

  Example:
      >>> parse_date("2025-01-15")
      (2025, 1, 'Winter')
  """
  try:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.year
    month = dt.month

    # Determine season based on month
    if month in [12, 1, 2]:
      season = "Winter"
    elif month in [3, 4, 5]:
      season = "Spring"
    elif month in [6, 7, 8]:
      season = "Summer"
    else:  # months 9, 10, 11
      season = "Autumn"

    return year, month, season
  except ValueError:
    return None, None, None


def parse_frshtt(frshtt_str):
  """
  Parse FRSHTT binary indicator string to extract extreme weather event flags.

  FRSHTT is a 6-character string where each position represents a weather event:
    Position 0: Fog (F)
    Position 1: Rain (R)
    Position 2: Snow (S)
    Position 3: Hail (H)
    Position 4: Thunder (T)
    Position 5: Tornado (T)

  Each position contains '1' if the event occurred, '0' otherwise.

  Args:
      frshtt_str (str): Six-character binary string (e.g., "001010")

  Returns:
      dict: Dictionary mapping event names to boolean values
          Example: {"Fog": False, "Rain": False, "Snow": True, ...}
          Returns empty dict if string length is not 6

  Example:
      >>> parse_frshtt("001010")
      {'Fog': False, 'Rain': False, 'Snow': True, 'Hail': False, 'Thunder': True, 'Tornado': False}
  """
  if len(frshtt_str) != 6:
    return {}

  flags = ["Fog", "Rain", "Snow", "Hail", "Thunder", "Tornado"]
  events = {}
  for i, flag in enumerate(flags):
    events[flag] = frshtt_str[i] == "1"

  return events


def is_valid_record(record):
  """
  Validate climate record by checking if numeric fields contain valid values.

  NOAA GSOD data uses 999.9 or 9999.9 as missing value indicators.
  This function filters out records with these invalid markers.

  Args:
      record (dict): Dictionary containing climate observation fields:
          - TEMP: Mean temperature
          - MAX: Maximum temperature
          - MIN: Minimum temperature
          - PRCP: Precipitation amount
          - WDSP: Mean wind speed
          - GUST: Maximum wind gust speed

  Returns:
      bool: True if all numeric fields contain valid values (< 999),
            False if any field is missing, invalid, or cannot be converted to float

  Note:
      This function is designed to be used as a filter in RDD transformations.
  """
  try:
    # Parse and strip whitespace from numeric fields
    temp = float(record["TEMP"].strip())
    max_temp = float(record["MAX"].strip())
    min_temp = float(record["MIN"].strip())
    prcp = float(record["PRCP"].strip())
    wdsp = float(record["WDSP"].strip())
    gust = float(record["GUST"].strip())

    # Check if values are reasonable (not missing/invalid markers like 999.9 or 9999.9)
    # Using < 999 threshold to catch both 999.9 and 9999.9
    return (
      abs(temp) < 999
      and abs(max_temp) < 999
      and abs(min_temp) < 999
      and prcp < 999
      and wdsp < 999
      and gust < 999
    )
  except (ValueError, KeyError):
    return False


def main(input_path, output_path):
  """
  Main execution function for climate data analysis pipeline.

  This function orchestrates the entire climate data analysis workflow:
  1. Initializes Spark context with appropriate configuration
  2. Loads GSOD CSV data into RDDs
  3. Cleans and validates data, handling inconsistent CSV formats
  4. Transforms data into structured records with parsed dates and events
  5. Performs aggregations for climate metrics
  6. Computes summary statistics
  7. Saves results to output directory

  Args:
      input_path (str): Path to input GSOD CSV file(s). Can be:
          - Single file path (e.g., "data/station.csv")
          - Directory containing multiple CSV files (e.g., "data/")
          - Glob pattern for multiple files (e.g., "data/*.csv")
      output_path (str): Directory path where analysis results will be saved.
          Creates subdirectories for different result types.

  Outputs:
      Creates the following directories in output_path:
          - monthly_avg_temp/: Monthly average temperatures per station
          - yearly_avg_temp/: Yearly average temperatures per station
          - seasonal_prcp/: Seasonal precipitation averages
          - highest_max_temp/: Top 10 stations with highest maximum temperatures
          - extreme_events/: Count of extreme weather events per station
          - summary/: Summary statistics (hottest year, wettest station, highest gust)

  Note:
      The function handles CSV files with inconsistent formatting where station names
      may contain commas, causing field count variations (28 vs 29 fields).
  """
  # Initialize SparkContext with configuration optimized for local execution
  # and cross-platform compatibility (Windows/Linux)
  conf = (
    SparkConf()
    .setAppName("ClimateDataAnalysis")
    .setMaster("local[*]")  # Use all available cores
    .set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .set("spark.hadoop.fs.defaultFS", "file:///")
    .set("spark.python.worker.faulthandler.enabled", "true")
  )
  sc = SparkContext(conf=conf)

  # ============================================================================
  # Step 1: Data Loading
  # ============================================================================
  raw_data = sc.textFile(input_path)

  if raw_data.isEmpty():
    print(f"ERROR: No data loaded from {input_path}")
    sc.stop()
    return

  # Remove header row (first line contains column names)
  header = raw_data.first()
  data_without_header = raw_data.filter(lambda line: line != header)

  # Parse CSV lines using Python's csv module to handle quoted fields
  parsed_data = data_without_header.map(lambda line: next(csv.reader([line])))

  # ============================================================================
  # Step 2: Data Cleaning and Field Extraction
  # ============================================================================

  def extract_fields(fields):
    """
    Extract relevant climate fields from CSV row.

    Handles two CSV formats due to inconsistent station name formatting:
    - 28 fields: Station name without embedded comma
    - 29 fields: Station name contains comma (e.g., "CITY NAME, COUNTRY")

    Args:
        fields (list): List of CSV field values

    Returns:
        dict: Dictionary with extracted fields, or None if extraction fails
    """
    try:
      # Determine format based on field count and extract accordingly
      if len(fields) == 29:
        # Format with comma in station name (fields shifted by 1)
        return {
          "STATION": fields[0].strip().strip('"'),
          "DATE": fields[1].strip().strip('"'),
          "TEMP": fields[7].strip().strip('"'),
          "MAX": fields[21].strip().strip('"'),
          "MIN": fields[23].strip().strip('"'),
          "PRCP": fields[25].strip().strip('"'),
          "WDSP": fields[17].strip().strip('"'),
          "GUST": fields[20].strip().strip('"'),
          "FRSHTT": fields[28].strip().strip('"'),
        }
      elif len(fields) == 28:
        # Standard format without comma in station name
        return {
          "STATION": fields[0].strip().strip('"'),
          "DATE": fields[1].strip().strip('"'),
          "TEMP": fields[6].strip().strip('"'),
          "MAX": fields[20].strip().strip('"'),
          "MIN": fields[22].strip().strip('"'),
          "PRCP": fields[24].strip().strip('"'),
          "WDSP": fields[16].strip().strip('"'),
          "GUST": fields[19].strip().strip('"'),
          "FRSHTT": fields[27].strip().strip('"'),
        }
      else:
        # Unknown format, skip this record
        return None
    except (IndexError, ValueError):
      return None

  # Apply field extraction and filter out invalid records
  cleaned_data = parsed_data.map(extract_fields).filter(lambda x: x is not None)
  cleaned_data = cleaned_data.filter(is_valid_record)

  if cleaned_data.isEmpty():
    print("ERROR: No valid records after cleaning!")
    sc.stop()
    return

  # ============================================================================
  # Step 3: Data Transformation
  # ============================================================================

  # Transform cleaned records into structured format with parsed dates and numeric values
  transformed_data = cleaned_data.map(
    lambda record: {
      "station": record["STATION"],
      "year": parse_date(record["DATE"])[0],
      "month": parse_date(record["DATE"])[1],
      "season": parse_date(record["DATE"])[2],
      "temp": float(record["TEMP"].strip()),
      "max_temp": float(record["MAX"].strip()),
      "min_temp": float(record["MIN"].strip()),
      "prcp": float(record["PRCP"].strip()),
      "wdsp": float(record["WDSP"].strip()),
      "gust": float(record["GUST"].strip()),
      "events": parse_frshtt(record["FRSHTT"]),
    }
  ).filter(lambda r: r["year"] is not None)  # Remove records with invalid dates

  # ============================================================================
  # Step 4: Aggregations and Climate Analysis
  # ============================================================================

  # 1. Monthly average temperatures per station
  # Calculate mean temperature for each station-year-month combination
  monthly_avg_temp = (
    transformed_data.map(
      lambda r: ((r["station"], r["year"], r["month"]), (r["temp"], 1))
    )
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Sum temps and counts
    .mapValues(lambda v: v[0] / v[1])  # Calculate average
  )

  # 2. Yearly average temperatures per station
  # Calculate mean temperature for each station-year combination
  yearly_avg_temp = (
    transformed_data.map(lambda r: ((r["station"], r["year"]), (r["temp"], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: v[0] / v[1])
  )

  # 3. Seasonal precipitation averages
  # Calculate mean precipitation for each station-year-season combination
  seasonal_prcp = (
    transformed_data.map(
      lambda r: ((r["station"], r["year"], r["season"]), (r["prcp"], 1))
    )
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: v[0] / v[1])
  )

  # 4. Stations with highest maximum daily temperatures
  # Find the highest recorded maximum temperature for each station and take top 10
  highest_max_temp = (
    transformed_data.map(lambda r: (r["station"], r["max_temp"]))
    .reduceByKey(max)  # Get maximum temp for each station
    .sortBy(lambda x: x[1], ascending=False)  # Sort by temperature descending
    .take(10)  # Collect top 10 stations
  )

  # 5. Extreme weather events count per station and event type
  # Count occurrences of each event type (Fog, Rain, Snow, Hail, Thunder, Tornado)
  extreme_events = transformed_data.flatMap(
    lambda r: [
      ((r["station"], event), 1) for event in r["events"] if r["events"][event]
    ]
  ).reduceByKey(lambda a, b: a + b)

  # ============================================================================
  # Step 5: Summary Statistics
  # ============================================================================

  # Compute hottest year (average temperature across all stations for each year)
  try:
    hottest_year = (
      yearly_avg_temp.map(lambda x: (x[0][1], (x[1], 1)))  # Extract year
      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Aggregate by year
      .mapValues(lambda v: v[0] / v[1])  # Calculate mean across stations
      .sortBy(lambda x: x[1], ascending=False)
      .first()
    )
  except ValueError:
    hottest_year = (None, 0.0)

  # Compute wettest station (total precipitation across all observations)
  try:
    wettest_station = (
      transformed_data.map(lambda r: (r["station"], r["prcp"]))
      .reduceByKey(lambda a, b: a + b)  # Sum all precipitation
      .sortBy(lambda x: x[1], ascending=False)
      .first()
    )
  except ValueError:
    wettest_station = (None, 0.0)

  # Compute highest wind gust recorded across all stations
  try:
    highest_gust = (
      transformed_data.map(lambda r: (r["station"], r["gust"]))
      .reduceByKey(max)  # Find maximum gust per station
      .sortBy(lambda x: x[1], ascending=False)
      .first()
    )
  except ValueError:
    highest_gust = (None, 0.0)

  # ============================================================================
  # Step 6: Saving Results
  # ============================================================================

  print("Saving results to", output_path)

  # Remove existing output directories to allow overwriting
  output_dirs = [
    "monthly_avg_temp",
    "yearly_avg_temp",
    "seasonal_prcp",
    "highest_max_temp",
    "extreme_events",
    "summary",
  ]

  for dir_name in output_dirs:
    dir_path = os.path.join(output_path, dir_name)
    if os.path.exists(dir_path):
      shutil.rmtree(dir_path)

  # Save monthly average temperatures (format: station,year,month,avg_temp)
  monthly_avg_temp.map(lambda x: f"{x[0][0]},{x[0][1]},{x[0][2]},{x[1]}").coalesce(
    1
  ).saveAsTextFile(output_path + "/monthly_avg_temp")

  # Save yearly average temperatures (format: station,year,avg_temp)
  yearly_avg_temp.map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}").coalesce(
    1
  ).saveAsTextFile(output_path + "/yearly_avg_temp")

  # Save seasonal precipitation averages (format: station,year,season,avg_prcp)
  seasonal_prcp.map(lambda x: f"{x[0][0]},{x[0][1]},{x[0][2]},{x[1]}").coalesce(
    1
  ).saveAsTextFile(output_path + "/seasonal_prcp")

  # Save top 10 stations with highest maximum temperatures (format: station,max_temp)
  sc.parallelize(highest_max_temp).map(lambda x: f"{x[0]},{x[1]}").coalesce(
    1
  ).saveAsTextFile(output_path + "/highest_max_temp")

  # Save extreme events counts (format: station,event_type,count)
  extreme_events.map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}").coalesce(
    1
  ).saveAsTextFile(output_path + "/extreme_events")

  # Save summary statistics as human-readable text
  summary = sc.parallelize(
    [
      f"Hottest year: {hottest_year[0]} with avg temp {hottest_year[1]:.2f}",
      f"Wettest station: {wettest_station[0]} with total prcp {wettest_station[1]:.2f}",
      f"Highest gust: {highest_gust[0]} with {highest_gust[1]:.2f}",
    ]
  )
  summary.coalesce(1).saveAsTextFile(output_path + "/summary")

  print("Analysis complete! Results saved successfully.")
  sc.stop()


if __name__ == "__main__":
  # Configure command-line argument parser
  parser = argparse.ArgumentParser(
    description="Analyze NOAA GSOD climate data using Apache Spark",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Run with default paths
  python -m ii3502_lab6.climate_analysis
  
  # Run with custom input/output paths
  python -m ii3502_lab6.climate_analysis --input data/2025/ --output results/
  
  # Run with specific file
  python -m ii3502_lab6.climate_analysis --input data/station.csv --output results/
    """,
  )

  parser.add_argument(
    "--input",
    default="src/main/resources/data/",
    help="Input path for GSOD CSV files. Can be a file, directory, or glob pattern (default: src/main/resources/data/)",
  )

  parser.add_argument(
    "--output",
    default="src/main/resources/output/",
    help="Output directory for analysis results (default: src/main/resources/output/)",
  )

  args = parser.parse_args()

  # Ensure output directory exists before starting analysis
  os.makedirs(args.output, exist_ok=True)

  # Execute main analysis pipeline
  main(args.input, args.output)
