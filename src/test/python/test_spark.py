import unittest
from pyspark import SparkContext, SparkConf
import os

class TestSparkBasic(unittest.TestCase):

    def setUp(self):
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
        data = self.sc.textFile("src/main/resources/data/sample_2025.csv")
        count = data.count()
        self.assertEqual(count, 4)  # header + 3 lines

if __name__ == '__main__':
    unittest.main()
