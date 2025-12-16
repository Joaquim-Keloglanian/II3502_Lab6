#!/usr/bin/env python3
"""
Station Map Visualization Module
==================================

This module creates an interactive HTML map displaying weather station locations
from NOAA GSOD climate data. It extracts station coordinates (latitude/longitude)
and generates a Folium map with markers for each station.

Features:
    - Extract station locations from GSOD CSV files
    - Handle multiple CSV formats (with/without comma in station names)
    - Create interactive HTML map with station markers
    - Display station information in popup tooltips
    - Export map to HTML file for viewing in browser

Author: Joaquim Keloglanian
Course: II3502 - Distributed Architectures and Programming
Institution: ISEP
Date: December 2025
"""

import csv
import folium
import argparse
import os
import glob


def extract_station_locations(file_paths):
  """
  Extract unique station locations from GSOD CSV files.

  Reads one or more CSV files and extracts station ID, name, latitude,
  longitude, and elevation for each unique station. Handles CSV format
  variations where station names may contain commas.

  OPTIMIZED: Only reads the first data row per file since all rows in a
  single file belong to the same station. This dramatically improves
  performance for large datasets.

  Args:
      file_paths (list): List of file paths to CSV files

  Returns:
      dict: Dictionary mapping station IDs to location information:
          {
              station_id: {
                  'name': str,
                  'latitude': float,
                  'longitude': float,
                  'elevation': float
              }
          }

  Note:
      - Skips header rows automatically
      - Handles both 28-field and 29-field CSV formats
      - Returns only unique stations (one entry per station ID)
      - Optimized for large datasets by reading only first row per file
  """
  stations = {}
  processed_files = 0
  failed_files = 0

  print(f"Extracting station metadata from {len(file_paths)} file(s)...")

  for file_path in file_paths:
    try:
      with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header

        # OPTIMIZATION: Only read first data row since all rows in file
        # belong to same station (saves massive time on large datasets)
        try:
          row = next(reader)
        except StopIteration:
          # Empty file (only header)
          failed_files += 1
          continue

        try:
          # Handle different CSV formats
          if len(row) == 29:
            # Format with comma in station name (fields 5 and 6 split by comma)
            station_id = row[0].strip().strip('"')
            latitude = float(row[2].strip().strip('"'))
            longitude = float(row[3].strip().strip('"'))
            elevation = float(row[4].strip().strip('"'))
            # Name is split across fields 5 and 6, rejoin with comma
            name_part1 = row[5].strip().strip('"')
            name_part2 = row[6].strip().strip('"')
            name = f"{name_part1}, {name_part2}"
          elif len(row) == 28:
            # Standard format
            station_id = row[0].strip().strip('"')
            latitude = float(row[2].strip().strip('"'))
            longitude = float(row[3].strip().strip('"'))
            elevation = float(row[4].strip().strip('"'))
            name = row[5].strip().strip('"')
          else:
            failed_files += 1
            continue

          # Store station info (only if not already present)
          if station_id not in stations:
            stations[station_id] = {
              "name": name,
              "latitude": latitude,
              "longitude": longitude,
              "elevation": elevation,
            }
            processed_files += 1

        except (ValueError, IndexError) as e:
          print(f"Warning: Could not parse data in {file_path}: {e}")
          failed_files += 1
          continue

    except Exception as e:
      print(f"Warning: Could not process file {file_path}: {e}")
      failed_files += 1
      continue

  print(f"Successfully extracted {len(stations)} unique station(s)")
  if failed_files > 0:
    print(f"Warning: {failed_files} file(s) could not be processed")

  return stations


def create_station_map(stations, output_file="station_map.html"):
  """
  Create an interactive HTML map with station markers.

  Generates a Folium map centered on the mean location of all stations,
  with markers for each station showing detailed information on click.

  Args:
      stations (dict): Dictionary of station information from extract_station_locations()
      output_file (str): Output path for HTML map file (default: "station_map.html")

  Returns:
      folium.Map: The generated Folium map object

  Map Features:
      - Centered on mean coordinates of all stations
      - Zoom level automatically adjusted for data extent
      - Markers with popup containing:
          * Station ID
          * Station name
          * Coordinates (lat/lon)
          * Elevation
      - OpenStreetMap base layer
      - Click markers to see station details

  Example:
      >>> stations = extract_station_locations(["data/*.csv"])
      >>> map_obj = create_station_map(stations, "output/stations.html")
      >>> # Opens stations.html in browser to view interactive map
  """
  if not stations:
    print("No stations to display on map!")
    return None

  # Calculate center point (mean of all latitudes and longitudes)
  mean_lat = sum(s["latitude"] for s in stations.values()) / len(stations)
  mean_lon = sum(s["longitude"] for s in stations.values()) / len(stations)

  # Create map centered on mean location
  station_map = folium.Map(
    location=[mean_lat, mean_lon],
    zoom_start=4,  # Adjust zoom level as needed
    tiles="OpenStreetMap",
  )

  # Add markers for each station
  for station_id, info in stations.items():
    popup_html = f"""
        <div style="font-family: Arial, sans-serif; width: 200px;">
            <h4 style="margin: 0 0 10px 0; color: #2c3e50;">{info["name"]}</h4>
            <table style="width: 100%; font-size: 12px;">
                <tr>
                    <td style="font-weight: bold;">Station ID:</td>
                    <td>{station_id}</td>
                </tr>
                <tr>
                    <td style="font-weight: bold;">Latitude:</td>
                    <td>{info["latitude"]:.4f}°</td>
                </tr>
                <tr>
                    <td style="font-weight: bold;">Longitude:</td>
                    <td>{info["longitude"]:.4f}°</td>
                </tr>
                <tr>
                    <td style="font-weight: bold;">Elevation:</td>
                    <td>{info["elevation"]:.1f} m</td>
                </tr>
            </table>
        </div>
        """

    folium.Marker(
      location=[info["latitude"], info["longitude"]],
      popup=folium.Popup(popup_html, max_width=250),
      tooltip=f"{info['name']} ({station_id})",
      icon=folium.Icon(color="blue", icon="cloud"),
    ).add_to(station_map)

  # Save map to HTML file
  station_map.save(output_file)
  print(f"Map saved to: {output_file}")
  print(f"Total stations displayed: {len(stations)}")

  return station_map


def main():
  """
  Main function for command-line usage of station map visualization.

  Parses command-line arguments, extracts station locations from CSV files,
  and generates an interactive HTML map.

  Command-line Arguments:
      --input: Path to input CSV file(s). Can be:
          - Single file: data/station.csv
          - Directory: data/ (processes all CSV files)
          - Glob pattern: data/*.csv
          Default: src/main/resources/data/*.csv

      --output: Output path for HTML map file
          Default: src/main/resources/output/station_map.html

  Examples:
      # Generate map from default data directory
      python -m ii3502_lab6.station_map

      # Generate map from specific directory
      python -m ii3502_lab6.station_map --input data/2025/ --output maps/2025.html

      # Generate map from single file
      python -m ii3502_lab6.station_map --input data/station.csv --output station.html
  """
  parser = argparse.ArgumentParser(
    description="Generate interactive map of weather station locations",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Generate map with default paths
  python -m ii3502_lab6.station_map
  
  # Generate map from specific directory
  python -m ii3502_lab6.station_map --input data/2025/ --output maps/2025_stations.html
  
  # Generate map from single file
  python -m ii3502_lab6.station_map --input data/station.csv --output station_map.html
        """,
  )

  parser.add_argument(
    "--input",
    default="src/main/resources/data/",
    help="Input path for GSOD CSV files (file, directory, or glob pattern)",
  )

  parser.add_argument(
    "--output",
    default="src/main/resources/output/station_map.html",
    help="Output path for HTML map file",
  )

  args = parser.parse_args()

  # Resolve input files
  input_path = args.input
  if os.path.isdir(input_path):
    # If directory, get all CSV files
    file_paths = glob.glob(os.path.join(input_path, "*.csv"))
  elif "*" in input_path:
    # If glob pattern, expand it
    file_paths = glob.glob(input_path)
  else:
    # Single file
    file_paths = [input_path]

  if not file_paths:
    print(f"Error: No CSV files found at {input_path}")
    return

  print(f"Processing {len(file_paths)} CSV file(s)...")

  # Extract station locations
  stations = extract_station_locations(file_paths)

  if not stations:
    print("Error: No valid station data found in input files")
    return

  # Create output directory if needed
  output_dir = os.path.dirname(args.output)
  if output_dir and not os.path.exists(output_dir):
    os.makedirs(output_dir, exist_ok=True)

  # Create and save map
  create_station_map(stations, args.output)


if __name__ == "__main__":
  main()
