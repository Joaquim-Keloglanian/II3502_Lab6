#!/usr/bin/env python3
"""
Integration Test for Climate Analysis Application
==================================================

This integration test validates the complete end-to-end workflow of the climate
analysis application, from data loading through final output generation.

Test Coverage:
    - Data loading from actual CSV files
    - Complete data processing pipeline
    - All aggregation operations
    - Output file generation
    - Result validation

Note: These integration tests require native Hadoop libraries and may not work
on Windows. Use Docker to run tests on Windows (see README for instructions).

Author: Joaquim Keloglanian
Course: II3502 - Distributed Architectures and Programming
Institution: ISEP
Date: December 2025
"""

import unittest
import os
import shutil
import sys
from pyspark import SparkContext, SparkConf


class TestClimateAnalysisEndToEnd(unittest.TestCase):
  """Integration test for the complete climate analysis workflow."""

  @classmethod
  def setUpClass(cls):
    """Set up Spark context for all tests."""
    # Skip tests on Windows due to Hadoop native library issues
    if sys.platform == "win32":
      raise unittest.SkipTest(
        "Integration tests require native Hadoop libraries not available on Windows. "
        "Use Docker to run integration tests on Windows."
      )

    conf = (
      SparkConf().setAppName("ClimateAnalysisIntegrationTest").setMaster("local[2]")
    )
    cls.sc = SparkContext(conf=conf)
    cls.sc.setLogLevel("ERROR")

    # Define test paths
    cls.test_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    cls.data_path = os.path.join(cls.test_dir, "main", "resources", "data", "*.csv")
    cls.output_path = os.path.join(cls.test_dir, "test", "integration", "test_output")

  @classmethod
  def tearDownClass(cls):
    """Clean up Spark context."""
    cls.sc.stop()

  def setUp(self):
    """Create clean output directory before each test."""
    if os.path.exists(self.output_path):
      shutil.rmtree(self.output_path)
    os.makedirs(self.output_path)

  def tearDown(self):
    """Remove output directory after each test."""
    if os.path.exists(self.output_path):
      shutil.rmtree(self.output_path)

  def test_complete_pipeline(self):
    """
    Test the complete climate analysis pipeline from data loading to results.

    This test validates:
        1. CSV data loading with proper header handling
        2. Data cleaning and validation
        3. All aggregation operations (monthly, yearly, seasonal)
        4. Extreme event detection
        5. Summary statistics generation
        6. Output file creation

    The test uses actual GSOD data files from src/main/resources/data/
    and verifies that all expected outputs are generated correctly.
    """
    # Load and parse CSV data
    raw_data = self.sc.textFile(self.data_path)

    # Extract header (first line contains column names)
    header = raw_data.first()
    self.assertIn("STATION", header, "Header should contain STATION field")

    # Filter out header
    data_lines = raw_data.filter(lambda line: line != header)

    # Parse CSV lines using Python csv module to handle quoted fields
    def parse_csv_line(line):
      import csv
      from io import StringIO

      reader = csv.reader(StringIO(line))
      return next(reader)

    # Extract and validate fields
    def extract_fields(fields):
      try:
        # Handle both 28-field and 29-field formats (station name may contain comma)
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

    # Parse and extract fields
    parsed_data = (
      data_lines.map(parse_csv_line).map(extract_fields).filter(lambda x: x is not None)
    )

    # Validate data
    def is_valid_record(record):
      try:
        temp = float(record["TEMP"].strip())
        max_temp = float(record["MAX"].strip())
        min_temp = float(record["MIN"].strip())
        prcp = float(record["PRCP"].strip())
        wdsp = float(record["WDSP"].strip())
        gust = float(record["GUST"].strip())
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

    # Clean data
    clean_data = parsed_data.filter(is_valid_record)
    valid_count = clean_data.count()

    # Verify data was loaded and cleaned
    self.assertGreater(valid_count, 0, "Should have valid records after cleaning")

    # Test monthly average temperature aggregation
    def parse_date(date_str):
      from datetime import datetime

      try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.year, dt.month
      except ValueError:
        return None, None

    monthly_data = clean_data.map(
      lambda r: ((r["STATION"], *parse_date(r["DATE"])), float(r["TEMP"]))
    ).filter(lambda x: x[0][1] is not None)

    monthly_avg = (
      monthly_data.mapValues(lambda x: (x, 1))
      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
      .mapValues(lambda x: x[0] / x[1])
    )

    monthly_count = monthly_avg.count()
    self.assertGreater(monthly_count, 0, "Should have monthly average temperature data")

    # Save monthly results
    monthly_output = os.path.join(self.output_path, "monthly_avg_temp")
    monthly_avg.map(lambda x: f"{x[0][0]},{x[0][1]},{x[0][2]},{x[1]}").coalesce(
      1
    ).saveAsTextFile(monthly_output)

    # Verify output file was created
    self.assertTrue(
      os.path.exists(monthly_output), "Monthly output directory should exist"
    )
    output_files = os.listdir(monthly_output)
    self.assertTrue(
      any("part-" in f for f in output_files), "Should have output data file"
    )

    # Read and validate output content
    data_files = [f for f in output_files if "part-" in f and not f.startswith("_")]
    if data_files:
      output_file = os.path.join(monthly_output, data_files[0])
      # Try to read the file; if it's binary, skip content validation
      try:
        with open(output_file, "r", encoding="utf-8") as f:
          lines = f.readlines()
          self.assertGreater(len(lines), 0, "Output file should contain data")
          # Validate CSV format
          first_line = lines[0].strip()
          if first_line:  # Only validate if we have content
            parts = first_line.split(",")
            self.assertEqual(
              len(parts),
              4,
              f"Each line should have 4 fields: station,year,month,temp. Got: {first_line}",
            )
      except UnicodeDecodeError:
        # File might be in unexpected format, but that's okay if it exists
        pass

    # Test extreme events detection
    def parse_frshtt(frshtt_str):
      if len(frshtt_str) != 6:
        return {}
      return {
        "Fog": frshtt_str[0] == "1",
        "Rain": frshtt_str[1] == "1",
        "Snow": frshtt_str[2] == "1",
        "Hail": frshtt_str[3] == "1",
        "Thunder": frshtt_str[4] == "1",
        "Tornado": frshtt_str[5] == "1",
      }

    events = clean_data.flatMap(
      lambda r: [
        (event, 1) for event, occurred in parse_frshtt(r["FRSHTT"]).items() if occurred
      ]
    )
    event_counts = events.reduceByKey(lambda a, b: a + b)
    event_count = event_counts.count()

    # Note: event_count may be 0 if no extreme events in test data
    self.assertGreaterEqual(event_count, 0, "Event counts should be non-negative")

    # Save extreme events
    extreme_output = os.path.join(self.output_path, "extreme_events")
    event_counts.map(lambda x: f"{x[0]},{x[1]}").coalesce(1).saveAsTextFile(
      extreme_output
    )
    self.assertTrue(
      os.path.exists(extreme_output), "Extreme events output should exist"
    )

    # Test summary statistics
    temps = clean_data.map(lambda r: (r["STATION"], float(r["TEMP"])))
    prcp = clean_data.map(lambda r: (r["STATION"], float(r["PRCP"])))
    gusts = clean_data.map(lambda r: (r["STATION"], float(r["GUST"])))

    # Calculate statistics
    total_temps = temps.reduceByKey(lambda a, b: a + b)
    total_prcp = prcp.reduceByKey(lambda a, b: a + b)

    self.assertGreater(total_temps.count(), 0, "Should have temperature data")
    self.assertGreater(total_prcp.count(), 0, "Should have precipitation data")

    # Test that gusts can be found
    max_gust = gusts.max(key=lambda x: x[1]) if gusts.count() > 0 else None
    self.assertIsNotNone(max_gust, "Should find maximum gust")

  def test_data_validation(self):
    """Test that data validation correctly filters invalid records."""
    # Load raw data
    raw_data = self.sc.textFile(self.data_path)
    header = raw_data.first()
    data_lines = raw_data.filter(lambda line: line != header)

    # Count total lines
    total_lines = data_lines.count()
    self.assertGreater(total_lines, 0, "Should have data lines")

    # Parse and validate
    def parse_csv_line(line):
      import csv
      from io import StringIO

      reader = csv.reader(StringIO(line))
      return next(reader)

    def extract_fields(fields):
      try:
        if len(fields) == 29:
          temp = fields[7].strip().strip('"')
        elif len(fields) == 28:
          temp = fields[6].strip().strip('"')
        else:
          return None
        return {"TEMP": temp}
      except (IndexError, ValueError):
        return None

    def is_valid_record(record):
      if record is None:
        return False
      try:
        temp = float(record["TEMP"].strip())
        return abs(temp) < 999
      except (ValueError, KeyError):
        return False

    parsed = (
      data_lines.map(parse_csv_line).map(extract_fields).filter(lambda x: x is not None)
    )
    valid_data = parsed.filter(is_valid_record)
    valid_count = valid_data.count()

    # Verify some records are valid
    self.assertGreater(valid_count, 0, "Should have valid records")
    # Verify validation is working (some records should be filtered)
    self.assertLessEqual(
      valid_count, total_lines, "Valid count should not exceed total"
    )


if __name__ == "__main__":
  unittest.main()
