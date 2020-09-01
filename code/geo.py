
import math
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType
from pyspark.sql import SQLContext

class GeoTweets:


    def get_counter_tweets(self, orig_lon, orig_lat, dist):

        lon1 = orig_lon - dist / abs(math.cos(math.radians(orig_lat)) * 69);
        lon2 = orig_lon + dist / abs(math.cos(math.radians(orig_lat)) * 69);
        lat1 = orig_lat - (dist / 69);
        lat2 = orig_lat + (dist / 69);

        df_geo_sql = self.spark.sql(" select count(dss.distance) as counter "
                               "from ( "
                               "SELECT 3956 * 2 * ASIN(SQRT(POWER(SIN((" + str(
            orig_lat) + "- (lat)) * pi()/180 / 2),2) + COS(" + str(
            orig_lat) + "* pi()/180 ) * COS((lat) *pi()/180) * POWER(SIN((" + str(
            orig_lon) + "- long) *pi()/180 / 2), 2) )) as distance "
                        "FROM tweets_geo "
                        "WHERE long between " + str(lon1) + " and " + str(lon2) + " and lat between " + str(
            lat1) + " and " + str(lat2) + " "
                                          "ORDER BY Distance ) dss "
                                          " where dss.distance < " + str(dist) + " ")

        return df_geo_sql.first()['counter']

    def __init__(self):
        self.spark = SparkSession.builder.appName('SparkSQL_API').getOrCreate()

        site_root = os.path.realpath(os.path.dirname(__file__))
        dataset_csv_url = os.path.join(site_root, 'static/data', "tweets_geo.csv")

        self.df_geo = self.spark.read.format('csv').option("header", "true").load(dataset_csv_url)
        self.df_geo.createOrReplaceTempView('tweets_geo')
