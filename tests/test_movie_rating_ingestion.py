import logging
#sys.path.insert(0,'..')
from base_test import PySparkTest
from argparse import Namespace
from JET_movie_ratings.ingestion import MovieRatingsBatchIngestion

"""
1. Navigate to JET_case_movie_ratings/tests/
2. export PYSPARK_PYTHON='/usr/bin/python3.7'; 
3. export PYSPARK_DRIVER_PYTHON='/usr/bin/python3.7'
4. python3 -m unittest test_movie_rating_ingestion.MovieRatingBatchIngestionTest
"""

class MovieRatingBatchIngestionTest(PySparkTest):
    """
    Tests for MovieRatingBatchIngestion
    """

    @classmethod
    def setUpClass(cls):
        """
        called before tests in an individual class are run
        :return:
        """
        logger = logging.getLogger('MovieRatingsBatchIngestion')
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
        cls.spark = cls.create_testing_pyspark_session()
        cls.movieRatingsBatchIngestion = MovieRatingsBatchIngestion(cls.spark, logger)

    def _read_data(self):
        """
        read data from json file
        :return: (data frame)
        """
        return self.spark.read.json('test_ratings.json' , schema=self.movieRatingsBatchIngestion.get_ratings_schema())

    def test_negative_transform(self):
        """
        Tests transform method
        checks weather actual data frame and expected data frame has same columns
        :return:
        """
        df = self._read_data()
        expected_df_columns = ['reviewerID', 'asin', 'ratings', 'rating_time']
        transform_df = self.movieRatingsBatchIngestion.transform_ratings(df)
        self.assertNotEqual(expected_df_columns, transform_df.columns, 'Actual and expected columns are equal')

    def test_transform(self):
        """
        Tests transform method
        checks weather actual data frame and expected data frame has same columns
        :return:
        """
        df = self._read_data()
        expected_df_columns = ['reviewerID', 'asin', 'ratings', 'rating_time', 'rating_dt', 'month', 'year']
        transform_df = self.movieRatingsBatchIngestion.transform_ratings(df)
        self.assertEqual(expected_df_columns, transform_df.columns, 'Actual and expected columns are equal')



