import unittest
from ii3502_lab6.climate_analysis import parse_date, parse_frshtt, is_valid_record


class TestClimateAnalysis(unittest.TestCase):
  def test_parse_date_valid(self):
    year, month, season = parse_date("2023-12-15")
    self.assertEqual(year, 2023)
    self.assertEqual(month, 12)
    self.assertEqual(season, "Winter")

  def test_parse_date_invalid(self):
    year, month, season = parse_date("invalid")
    self.assertIsNone(year)
    self.assertIsNone(month)
    self.assertIsNone(season)

  def test_parse_frshtt_valid(self):
    events = parse_frshtt("101010")
    expected = {
      "Fog": True,
      "Rain": False,
      "Snow": True,
      "Hail": False,
      "Thunder": True,
      "Tornado": False,
    }
    self.assertEqual(events, expected)

  def test_parse_frshtt_invalid(self):
    events = parse_frshtt("123")
    self.assertEqual(events, {})

  def test_is_valid_record_valid(self):
    record = {
      "TEMP": "20.5",
      "MAX": "25.0",
      "MIN": "15.0",
      "PRCP": "0.0",
      "WDSP": "5.0",
      "GUST": "10.0",
    }
    self.assertTrue(is_valid_record(record))

  def test_is_valid_record_invalid(self):
    record = {
      "TEMP": "9999.9",
      "MAX": "25.0",
      "MIN": "15.0",
      "PRCP": "0.0",
      "WDSP": "5.0",
      "GUST": "10.0",
    }
    self.assertFalse(is_valid_record(record))


if __name__ == "__main__":
  unittest.main()
