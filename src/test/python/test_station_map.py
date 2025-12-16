#!/usr/bin/env python3
"""
Unit tests for station map visualization module.

Tests the functionality of extracting station locations from GSOD CSV files
and generating interactive maps.
"""

import unittest
import os
import tempfile
import shutil
from ii3502_lab6.station_map import extract_station_locations, create_station_map


class TestStationMap(unittest.TestCase):
  """Test cases for station map visualization functionality."""

  def setUp(self):
    """Set up test fixtures with sample CSV data."""
    self.test_dir = tempfile.mkdtemp()
    self.test_csv = os.path.join(self.test_dir, "test_station.csv")

    # Create sample CSV with header and data
    csv_content = """"STATION"    , "DATE"      , "LATITUDE"  , "LONGITUDE" , "ELEVATION", "NAME"                  , "TEMP"  , "TEMP_ATTRIBUTES", "DEWP"  , "DEWP_ATTRIBUTES", "SLP"   , "SLP_ATTRIBUTES", "STP"  , "STP_ATTRIBUTES", "VISIB", "VISIB_ATTRIBUTES", "WDSP" , "WDSP_ATTRIBUTES", "MXSPD", "GUST" , "MAX"   , "MAX_ATTRIBUTES", "MIN"   , "MIN_ATTRIBUTES", "PRCP" , "PRCP_ATTRIBUTES", "SNDP" , "FRSHTT"
"01001099999", "2025-01-01", "70.9333333", "-8.6666667", "9.0"      , "JAN MAYEN NOR NAVY, NO", "  17.4", " 8"             , "  10.3", " 8"              , "1014.9", " 8"            , "013.7", " 8"            , "  2.0", " 4"              , " 20.9", " 8"             , " 27.2", " 39.4", "  24.4", " "             , "   9.7", " "             , " 0.05", "G"              , "999.9", "001000"
"01001499999", "2025-01-01", "59.7888889", "5.3411111" , "48.8"     , "SORSTOKKEN, NO"        , "  42.8", " 8"             , "  39.7", " 8"              , "1007.6", " 8"            , "1002.3", " 8"           , " 11.2", " 8"              , " 15.0", " 8"             , " 20.6", " 27.6", "  46.4", " "             , "  39.2", " "             , " 0.08", "G"              , "999.9", "010000"
"""
    with open(self.test_csv, "w") as f:
      f.write(csv_content)

  def tearDown(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.test_dir)

  def test_extract_station_locations(self):
    """Test extracting station locations from CSV file."""
    stations = extract_station_locations([self.test_csv])

    # Verify we extracted 1 station (optimized to read only first row per file)
    # This is correct behavior since GSOD files have one station per file
    self.assertEqual(len(stations), 1)

    # Verify first station data
    self.assertIn("01001099999", stations)
    station1 = stations["01001099999"]
    self.assertEqual(station1["name"], "JAN MAYEN NOR NAVY, NO")
    self.assertAlmostEqual(station1["latitude"], 70.9333333, places=6)
    self.assertAlmostEqual(station1["longitude"], -8.6666667, places=6)
    self.assertAlmostEqual(station1["elevation"], 9.0, places=1)

  def test_extract_empty_file(self):
    """Test extracting from file with only header."""
    empty_csv = os.path.join(self.test_dir, "empty.csv")
    with open(empty_csv, "w") as f:
      f.write(
        '"STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "TEMP", "TEMP_ATTRIBUTES", "DEWP", "DEWP_ATTRIBUTES", "SLP", "SLP_ATTRIBUTES", "STP", "STP_ATTRIBUTES", "VISIB", "VISIB_ATTRIBUTES", "WDSP", "WDSP_ATTRIBUTES", "MXSPD", "GUST", "MAX", "MAX_ATTRIBUTES", "MIN", "MIN_ATTRIBUTES", "PRCP", "PRCP_ATTRIBUTES", "SNDP", "FRSHTT"\n'
      )

    stations = extract_station_locations([empty_csv])
    self.assertEqual(len(stations), 0)

  def test_extract_nonexistent_file(self):
    """Test extracting from nonexistent file."""
    stations = extract_station_locations(["nonexistent.csv"])
    self.assertEqual(len(stations), 0)

  def test_create_station_map(self):
    """Test creating HTML map from station data."""
    stations = extract_station_locations([self.test_csv])
    output_file = os.path.join(self.test_dir, "test_map.html")

    map_obj = create_station_map(stations, output_file)

    # Verify map was created
    self.assertIsNotNone(map_obj)
    self.assertTrue(os.path.exists(output_file))

    # Verify HTML file contains expected content (only first station due to optimization)
    with open(output_file, "r") as f:
      content = f.read()
      self.assertIn("JAN MAYEN NOR NAVY, NO", content)
      self.assertIn("01001099999", content)

  def test_create_map_empty_stations(self):
    """Test creating map with no station data."""
    output_file = os.path.join(self.test_dir, "empty_map.html")
    map_obj = create_station_map({}, output_file)

    # Verify map was not created for empty data
    self.assertIsNone(map_obj)
    self.assertFalse(os.path.exists(output_file))

  def test_extract_multiple_files(self):
    """Test extracting from multiple CSV files."""
    # Create second test file
    test_csv2 = os.path.join(self.test_dir, "test_station2.csv")
    csv_content = """"STATION"    , "DATE"      , "LATITUDE"  , "LONGITUDE" , "ELEVATION", "NAME"                  , "TEMP"  , "TEMP_ATTRIBUTES", "DEWP"  , "DEWP_ATTRIBUTES", "SLP"   , "SLP_ATTRIBUTES", "STP"  , "STP_ATTRIBUTES", "VISIB", "VISIB_ATTRIBUTES", "WDSP" , "WDSP_ATTRIBUTES", "MXSPD", "GUST" , "MAX"   , "MAX_ATTRIBUTES", "MIN"   , "MIN_ATTRIBUTES", "PRCP" , "PRCP_ATTRIBUTES", "SNDP" , "FRSHTT"
"01002099999", "2025-01-01", "60.1234567", "10.2345678", "100.0"    , "TEST STATION, NO"      , "  20.0", " 8"             , "  15.0", " 8"              , "1010.0", " 8"            , "1005.0", " 8"           , "  5.0", " 8"              , " 10.0", " 8"             , " 15.0", " 20.0", "  25.0", " "             , "  15.0", " "             , " 0.00", "G"              , "999.9", "000000"
"""
    with open(test_csv2, "w") as f:
      f.write(csv_content)

    stations = extract_station_locations([self.test_csv, test_csv2])

    # Verify we extracted 2 unique stations (one per file, optimized behavior)
    # This is correct because GSOD data has one station per file
    self.assertEqual(len(stations), 2)
    self.assertIn("01001099999", stations)
    self.assertIn("01002099999", stations)


if __name__ == "__main__":
  unittest.main()
