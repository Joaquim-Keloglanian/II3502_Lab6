import unittest
import sys
from pyspark import SparkContext, SparkConf


class TestSparkBasic(unittest.TestCase):
  def setUp(self):
    # Skip Spark tests on Windows due to native library issues
    if sys.platform == "win32":
      self.skipTest(
        "Spark tests require native Hadoop libraries not available on Windows. "
        "Use Docker to run Spark tests on Windows."
      )

    conf = (
      SparkConf()
      .setAppName("Test")
      .setMaster("local[*]")
      .set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .set("spark.hadoop.fs.defaultFS", "file:///")
      .set("spark.python.worker.faulthandler.enabled", "true")
    )
    self.sc = SparkContext(conf=conf)

  def tearDown(self):
    self.sc.stop()

  def test_text_file_count(self):
    # Use an actual data file that exists
    data = self.sc.textFile("src/main/resources/data/01001099999.csv")
    count = data.count()
    # Just verify we can read the file and it has some data
    self.assertGreater(count, 0, "File should contain at least one line")


if __name__ == "__main__":
  unittest.main()
