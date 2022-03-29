import unittest

from pyspark.sql import SparkSession

class PySparkTest(unittest.TestCase):
    """
    Base class for pyspark tests
    """
    @classmethod
    def create_testing_pyspark_session(cls):
        """
        Create spark session
        :return:
        """
        return SparkSession.builder.appName('movie_ratings_test').enableHiveSupport().getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """
        called after tests in an individual class are run
        :return:
        """
        cls.spark.stop()
