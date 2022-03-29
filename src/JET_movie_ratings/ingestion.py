import logging

from abc import abstractmethod
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, ArrayType, DoubleType
from pyspark.sql.functions import from_unixtime, month, year ,udf, regexp_replace, col, current_timestamp


class HiveExecutionException(Exception):
    """Hive execution error """


class Ingestion():
    """
    Generic ingestion
    """
    def __init__(self,
                 spark,
                 logger):
        """
        Initialize class variables
        :param spark:
        """
        self.spark = spark
        self.reg_ex = r"([a-z*]\s\'\d+\s)|(\\\')|(\d\'s)|([a-z]\'s\s)|(\'\d+[a-z])|([a-z+]\s+\"+)"
        self.ratings_columns = ['reviewerID', 'asin', 'ratings',
                                'rating_time', 'rating_dt', 'year',
                                'month', 'meta_asin', 'title']
        self.partition_cols = ['year', 'month']
        self.logger = logger
        self.num_coalesce = 1

    @staticmethod
    def get_ratings_schema():
        """
        get ratings schema
        :return:
        """
        ratings_schema = StructType([
            StructField("reviewerID", StringType(), True),
            StructField("asin", StringType(), True),
            StructField("ratings", FloatType(), True),
            StructField("rating_time", LongType(), True)
        ])
        return ratings_schema

    @staticmethod
    def get_movies_meta_schema():
        """
        get movies meta schema
        :return:
        """
        movies_meta_schema = StructType([
            StructField("asin", StringType()),
            StructField("categories", ArrayType(ArrayType(StringType(), True)), True),
            StructField("title", StringType(), True),
            StructField("price", DoubleType(), True)
        ])
        return movies_meta_schema

    def transform_ratings(self,
                          ratings_df):
        """
        perform transformations on ratings
        :param ratings_df:
        :return:
        """
        self.logger.info("Transforming ratings data - generating ratings_dt, year and month")
        ratings_df = ratings_df.withColumn('rating_dt', from_unixtime(ratings_df.rating_time, format='yyyy-MM-dd'))
        ratings_df = ratings_df.withColumn('month', month(ratings_df.rating_dt))
        ratings_df = ratings_df.withColumn('year', year(ratings_df.rating_dt))
        ratings_df = ratings_df.distinct()
        ratings_df.printSchema()
        self.logger.info("Completed transformation of ratings data")
        return ratings_df

    def transform_meta_movies(self,
                              movies_meta_df):
        """
        perform transformations on meta movies data and clean up
        :param movies_meta_df: movies meta source data
        :return: final_movies_meta_df: cleansed movies meta data
        """
        self.logger.info("Transforming meta movies data - handles corrupt records by using regex")
        movies_meta_df.cache()
        movies_meta_filtered_df = movies_meta_df.filter(movies_meta_df["_corrupt_record"].isNull()).select('asin',
                                                                                                           'categories',
                                                                                                           'title',
                                                                                                           'price')
        movies_meta_df.filter(movies_meta_df["_corrupt_record"].isNotNull()).count()
        #movies_meta_df.filter(movies_meta_df['_corrupt_record'].isNotNull()).select('_corrupt_record').show(5, False)
        corrupted_records_df = movies_meta_df.filter(movies_meta_df["_corrupt_record"].isNotNull())
        corrupted_records_df = corrupted_records_df.withColumn('_corrupt_record',
                                                               regexp_replace(corrupted_records_df['_corrupt_record'],
                                                                              self.reg_ex,
                                                                              ""))

        def transform_row(row):
            try:
                obj = eval(row)
            except Exception as e:
                logging.warning('Failure to parse record {0} with error {1}'.format(str(row), str(e.args)))
                record = ()
            else:
                asin = obj.get('asin')
                categories = obj.get('categories')
                title = obj.get('title')
                price = obj.get('price')
                record = (asin,
                          categories,
                          title,
                          price)
                return record

        transform_udf = udf(transform_row, self.get_movies_meta_schema())
        corrupted_records_df = corrupted_records_df.withColumn('corrected_data',
                                                               transform_udf(corrupted_records_df['_corrupt_record']))
        corrupted_records_df_formatted = corrupted_records_df.filter(col('corrected_data').isNotNull()).select(
            'corrected_data.*')
        final_movies_meta_df = movies_meta_filtered_df.unionByName(corrupted_records_df_formatted)
        final_movies_meta_df = final_movies_meta_df.withColumnRenamed('asin', 'meta_asin')
        final_movies_meta_df = final_movies_meta_df.distinct()
        self.logger.info("Completed transformation of movies meta dataset")
        return final_movies_meta_df

    def transform(self,
                  movies_meta_df,
                  ratings_df):
        """
        perform transformations on movies meta and ratings data
        :param movies_meta_df: movies meta source data
        :param ratings_df: ratings source data
        :return: ratings_meta_df: combined data of ratings and meta
        """
        self.logger.info("Transform ratings and meta movies data")
        ratings_df = self.transform_ratings(ratings_df)
        final_movies_meta_df = self.transform_meta_movies(movies_meta_df)
        ratings_meta_df = ratings_df.join(final_movies_meta_df, ratings_df.asin == final_movies_meta_df.meta_asin,
                                          'inner').select(*self.ratings_columns)
        ratings_meta_df.show(10, False)
        self.logger.info("Completed transformation of ratings and movies meta datasets")
        return ratings_meta_df

    @abstractmethod
    def read(self):
        """
        read the source data
        :return:
        """
        pass

    @abstractmethod
    def save(self,
             ratings_meta_df):
        """
        save the data to storage layer
        :param ratings_meta_df:
        :return:
        """
        pass

    @abstractmethod
    def process(self):
        """
        process the movie ratings data
        :return:
        """
        pass


class MovieRatingsBatchIngestion(Ingestion):
    """
    Movie Ratings Batch Ingestion
    """
    def __init__(self, spark, logger):
        """
        Initialize class variables
        :param spark: spark_session
        """
        super().__init__(spark, logger)
        self.output_path = 'hdfs:///tmp/JET/batch_movies_meta/output/batch_id=1613301962/'

    def read(self):
        """
        read the data from source
        :return: movies_meta_data and ratings_data
        """
        self.logger.info("reading the ratings and movie meta from source files")
        movies_meta_df = (self.spark.read.format('json').
                          option("mode", "PERMISSIVE").
                          option("columnNameOfCorruptRecord", "_corrupt_record").
                          option('allowBackslashEscapingAnyCharacter','true').
                          load('hdfs:///tmp/JET/meta_movies/meta_Movies_and_TV.json.gz'))

        ratings_df = (self.spark.read.format('csv').
                      load('hdfs:///tmp/JET/movies_csv/ratings_Movies_and_TV.csv',
                           schema=self.get_ratings_schema()))
        return movies_meta_df, ratings_df

    def save(self,
             ratings_meta_df):
        """
        save the ratings and meta to storage, below is the sample data once data is transformed
        +--------------+----------+-------+-----------+----------+----+-----+----------+----------------------------+
        |reviewerID    |asin      |ratings|rating_time|rating_dt |year|month|meta_asin |title                       |
        +--------------+----------+-------+-----------+----------+----+-----+----------+----------------------------+
        |A3TMUPDDXN8032|0738920630|4.0    |1283126400 |2010-08-30|2010|8    |0738920630|Works - Fun and Games [VHS] |
        |A3UTPOE8HGE0RY|0738920630|4.0    |1104278400 |2004-12-29|2004|12   |0738920630|Works - Fun and Games [VHS] |
        +--------------+----------+-------+-----------+----------+----+-----+----------+----------------------------+
        :param ratings_meta_df: joined and transformed data of movie ratings and meta movies data
        :return: None
        """
        self.logger.info("Started saving ratings_meta_movies data to storage")
        ratings_meta_df.coalesce(1).write.parquet(path=self.output_path,
                                                  mode='overwrite',
                                                  partitionBy=self.partition_cols)
        self.logger.info("Completed save of ratings_meta_movies data")

    def process(self):
        """
        process movie ratings batch data
        :return:
        """
        movies_meta_df, ratings_df = self.read()
        ratings_meta_df = self.transform(movies_meta_df, ratings_df)
        self.save(ratings_meta_df)
        row_years = ratings_meta_df.select('year').distinct().collect()
        years = [r.year for r in row_years]
        self.logger.info(years)

class MovieRatingsStreamingIngestion(Ingestion):
    """
    Movie Ratings streaming ingestion
    """
    def __init__(self, spark, logger):
        """"""
        super().__init__(spark, logger)
        self.max_files_per_trigger = 1000
        self.watermark_time = '3 minutes'
        self.checkpoint_location = 'hdfs:///tmp/JET/stream_movies_meta/checkpoint_dir/'
        self.output_path = 'hdfs:///tmp/JET/stream_movies_meta/stream_output/'

    def read(self):
        """
        read movie ratings source data and movies meta data
        :return:
        """
        self.logger.info("reading the ratings and movie meta from source files")
        movies_meta_df = (self.spark.readStream.format('json').
                          option("mode", "PERMISSIVE").
                          option("columnNameOfCorruptRecord", "_corrupt_record").
                          option('allowBackslashEscapingAnyCharacter', 'true').
                          load('hdfs:///tmp/JET/meta_movies/meta_Movies_and_TV.json.gz'))

        ratings_df = (self.spark.readStream.format('csv').
                      option("maxFilesPerTrigger", int(self.max_files_per_trigger)).
                      load('hdfs:///tmp/JET/movies_csv/*'))
        return movies_meta_df, ratings_df

    def save(self,
             ratings_meta_df):
        """
        save processed data to storage, below is the sample data once data is transformed
        +--------------+----------+-------+-----------+----------+----+-----+----------+----------------------------+
        |reviewerID    |asin      |ratings|rating_time|rating_dt |year|month|meta_asin |title                       |
        +--------------+----------+-------+-----------+----------+----+-----+----------+----------------------------+
        |A3TMUPDDXN8032|0738920630|4.0    |1283126400 |2010-08-30|2010|8    |0738920630|Works - Fun and Games [VHS] |
        |A3UTPOE8HGE0RY|0738920630|4.0    |1104278400 |2004-12-29|2004|12   |0738920630|Works - Fun and Games [VHS] |
        +--------------+----------+-------+-----------+----------+----+-----+----------+----------------------------+
        :param ratings_meta_df:
        :return:
        """
        self.logger.info("Started saving ratings_meta_movies data to storage")
        # drop duplicates
        ratings_meta_df = ratings_meta_df.withColumn('processed_time', current_timestamp().cast('long') * 1000)
        ratings_meta_df = ratings_meta_df.withWatermark("processed_time", self.watermark_time).dropDuplicates(
            ['asin', 'reviewerID', 'processed_time'])
        ratings_meta_df = ratings_meta_df.drop('processed_time')
        stream_writer = (ratings_meta_df.coalesce(self.num_coalesce).writeStream.format("parquet").
                         option("checkpointLocation", self.checkpoint_location).
                         outputMode("append").partitionBy(*self.partition_cols))
        streaming_query = stream_writer.start(self.output_path)

    def process(self):
        """
        Process movie ratings
        :return:
        """
        movies_meta_df, ratings_df = self.read()
        ratings_meta_df = self.transform(movies_meta_df, ratings_df)
        self.save(ratings_meta_df)
