#!/usr/bin/env python3
"""
Spark Climate Data Analysis Application

This application analyzes NOAA GSOD climate data using RDD-based transformations.
It loads, cleans, transforms, aggregates, and saves climate analysis results.
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime
import os
import argparse


def parse_date(date_str):
  """Parse date string to extract year, month, and season."""
  try:
    dt = datetime.strptime(date_str, "%Y%m%d")
    year = dt.year
    month = dt.month
    # Seasons: Winter (12,1,2), Spring (3,4,5), Summer (6,7,8), Autumn (9,10,11)
    if month in [12, 1, 2]:
      season = "Winter"
    elif month in [3, 4, 5]:
      season = "Spring"
    elif month in [6, 7, 8]:
      season = "Summer"
    else:
      season = "Autumn"
    return year, month, season
  except ValueError:
    return None, None, None


def parse_frshtt(frshtt_str):
  """Parse FRSHTT binary string to extract extreme event flags."""
  if len(frshtt_str) != 6:
    return {}
  flags = ["Fog", "Rain", "Snow", "Hail", "Thunder", "Tornado"]
  events = {}
  for i, flag in enumerate(flags):
    events[flag] = frshtt_str[i] == "1"
  return events


def is_valid_record(record):
  """Check if a record has valid numeric fields."""
  try:
    temp = float(record["TEMP"])
    max_temp = float(record["MAX"])
    min_temp = float(record["MIN"])
    prcp = float(record["PRCP"])
    wdsp = float(record["WDSP"])
    gust = float(record["GUST"])
    # Invalid values are often 9999.9 or similar
    invalid = 9999.9
    return (
      temp != invalid
      and max_temp != invalid
      and min_temp != invalid
      and prcp != invalid
      and wdsp != invalid
      and gust != invalid
    )
  except (ValueError, KeyError):
    return False


def main(input_path, output_path):
  """Main function to run the climate analysis."""
  # Initialize SparkContext with configuration for Windows compatibility
  conf = (
    SparkConf()
    .setAppName("ClimateDataAnalysis")
    .setMaster("local[*]")
    .set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .set("spark.hadoop.fs.defaultFS", "file:///")
    .set("spark.python.worker.faulthandler.enabled", "true")
  )
  sc = SparkContext(conf=conf)
  # Load CSV files into RDD
  raw_data = sc.textFile(input_path)

  # Skip header if present (assuming first line is header)
  header = raw_data.first()
  data_without_header = raw_data.filter(lambda line: line != header)

  # Parse CSV lines
  parsed_data = data_without_header.map(lambda line: line.split(","))

  # Extract relevant fields (assuming order: STATION, DATE, ..., TEMP, MAX, MIN, PRCP, WDSP, GUST, FRSHTT)
  # Note: Adjust indices based on actual CSV structure
  cleaned_data = parsed_data.map(
    lambda fields: {
      "STATION": fields[0],
      "DATE": fields[1],
      "TEMP": fields[8],  # Adjust index
      "MAX": fields[10],
      "MIN": fields[11],
      "PRCP": fields[12],
      "WDSP": fields[13],
      "GUST": fields[14],
      "FRSHTT": fields[15],
    }
  ).filter(is_valid_record)

  # Step 2: Data Cleaning and Transformation
  transformed_data = cleaned_data.map(
    lambda record: {
      "station": record["STATION"],
      "year": parse_date(record["DATE"])[0],
      "month": parse_date(record["DATE"])[1],
      "season": parse_date(record["DATE"])[2],
      "temp": float(record["TEMP"]),
      "max_temp": float(record["MAX"]),
      "min_temp": float(record["MIN"]),
      "prcp": float(record["PRCP"]),
      "wdsp": float(record["WDSP"]),
      "gust": float(record["GUST"]),
      "events": parse_frshtt(record["FRSHTT"]),
    }
  ).filter(lambda r: r["year"] is not None)

  # Step 4: Aggregations and Climate Analysis
  # 1. Monthly average temperatures per station
  monthly_avg_temp = (
    transformed_data.map(
      lambda r: ((r["station"], r["year"], r["month"]), (r["temp"], 1))
    )
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: v[0] / v[1])
  )

  # 2. Yearly average temperatures per station
  yearly_avg_temp = (
    transformed_data.map(lambda r: ((r["station"], r["year"]), (r["temp"], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: v[0] / v[1])
  )

  # 3. Seasonal precipitation averages
  seasonal_prcp = (
    transformed_data.map(
      lambda r: ((r["station"], r["year"], r["season"]), (r["prcp"], 1))
    )
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: v[0] / v[1])
  )

  # 4. Stations with highest max daily temperatures (top 10)
  highest_max_temp = (
    transformed_data.map(lambda r: (r["station"], r["max_temp"]))
    .reduceByKey(max)
    .sortBy(lambda x: x[1], ascending=False)
    .take(10)
  )

  # 5. Extreme events count per station and event type
  extreme_events = transformed_data.flatMap(
    lambda r: [
      ((r["station"], event), 1) for event in r["events"] if r["events"][event]
    ]
  ).reduceByKey(lambda a, b: a + b)

  # Summary statistics
  # Hottest year (average temp across all stations)
  hottest_year = (
    yearly_avg_temp.map(lambda x: (x[0][1], (x[1], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: v[0] / v[1])
    .sortBy(lambda x: x[1], ascending=False)
    .first()
  )

  # Wettest station (total precipitation)
  wettest_station = (
    transformed_data.map(lambda r: (r["station"], r["prcp"]))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
    .first()
  )

  # Highest wind gust
  highest_gust = (
    transformed_data.map(lambda r: (r["station"], r["gust"]))
    .reduceByKey(max)
    .sortBy(lambda x: x[1], ascending=False)
    .first()
  )

  # Step 5: Saving Results
  monthly_avg_temp.map(
    lambda x: f"{x[0][0]},{x[0][1]},{x[0][2]},{x[1]}"
  ).saveAsTextFile(output_path + "/monthly_avg_temp")

  yearly_avg_temp.map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}").saveAsTextFile(
    output_path + "/yearly_avg_temp"
  )

  seasonal_prcp.map(lambda x: f"{x[0][0]},{x[0][1]},{x[0][2]},{x[1]}").saveAsTextFile(
    output_path + "/seasonal_prcp"
  )

  sc.parallelize(highest_max_temp).map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile(
    output_path + "/highest_max_temp"
  )

  extreme_events.map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}").saveAsTextFile(
    output_path + "/extreme_events"
  )

  # Save summary
  summary = sc.parallelize(
    [
      f"Hottest year: {hottest_year[0]} with avg temp {hottest_year[1]:.2f}",
      f"Wettest station: {wettest_station[0]} with total prcp {wettest_station[1]:.2f}",
      f"Highest gust: {highest_gust[0]} with {highest_gust[1]:.2f}",
    ]
  )
  summary.saveAsTextFile(output_path + "/summary")

  sc.stop()


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Spark Climate Data Analysis")
  parser.add_argument(
    "--input",
    default="src/main/resources/data/",
    help="Input path for GSOD CSV files (default: src/main/resources/data/)",
  )
  parser.add_argument(
    "--output",
    default="src/main/resources/output/",
    help="Output directory for results (default: src/main/resources/output/)",
  )
  args = parser.parse_args()

  # Ensure output directory exists
  os.makedirs(args.output, exist_ok=True)

  main(args.input, args.output)
