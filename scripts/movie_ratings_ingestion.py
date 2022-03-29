import logging
import sys
import argparse

from JET_movie_ratings.ingestion import MovieRatingsBatchIngestion, MovieRatingsStreamingIngestion
from pyspark.sql import SparkSession

if __name__ == '__main__':

    def parse_args(params):
        """
        parser for parsing input arguments
        :param params:
        :return:
        """
        parser = argparse.ArgumentParser()

        parser.add_argument('--job_name',
                            type=str,
                            required=True,
                            help='name of the job')
        parser.add_argument('--action',
                            type=str,
                            choices=["ingest", "stream"],
                            default="ingest",
                            required=False,
                            help='which job to process')
        return parser.parse_args(params)


    arguments = parse_args(sys.argv[1:])
    logger = logging.getLogger(arguments.job_name)
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger.info(f"Parsed input arguments are : {arguments}")
    spark_session = SparkSession.builder.appName(arguments.job_name).enableHiveSupport().getOrCreate()
    spark_session.sparkContext.setLogLevel("WARN")
    if arguments.action == 'ingest':
        movieRatingsBatchIngestion = MovieRatingsBatchIngestion(spark_session, logger)
        movieRatingsBatchIngestion.process()
    elif arguments.action == 'stream':
        movieRatingsStreamingIngestion = MovieRatingsStreamingIngestion(spark_session, logger)
        movieRatingsStreamingIngestion.process()