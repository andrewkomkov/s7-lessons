# spark-submit verified_tags_candidates.py 2022-05-31 5 300 /user/username/data/events /user/username/data/snapshots/tags_verified/actual /user/username/data/analytics/verified_tags_candidates_d5

import sys
import datetime
import pyspark.sql.functions as F 
import dateutil.relativedelta
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F


date_from = sys.argv[1]
depth = sys.argv[2]
sugested_users = sys.argv[3]
input_path = sys.argv[4]
tags_verified_path = sys.argv[5]
output_path = sys.argv[6]

def input_paths(date,depth):
    range_ = []
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    i = 0
    while i < depth:
        date_2 = date - dateutil.relativedelta.relativedelta(days=i)
        path_ = f'{input_path}/date={date_2.strftime("%Y-%m-%d")}/event_type=message'
        range_.append(path_)
        i = i+1
    return(range_)
 
def main():
    conf = SparkConf().setAppName(f"VerifiedTagsCandidatesJob-{date_from}-d{depth}-cut{sugested_users}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    tags_verified = sql.read.parquet(tags_verified_path)


    range_ = input_paths(date_from,depth)

    df = sql.read.parquet(*range_)

    df_explode = df.withColumn("tag", F.explode(df.event.tags)).where("event.message_channel_to is not null")
    df_explode = df_explode.select(df_explode.event.message_from.alias("message_from"),df_explode.tag)

    df_explode = df_explode.groupBy('tag') \
                        .agg(F.countDistinct('message_from').alias("suggested_count")) \
                        .filter(F.col('suggested_count') >= sugested_users) \
                        .orderBy(F.col("suggested_count").desc()) \
                        .join(tags_verified, ['tag'], how='left_anti')

    df_explode.write.format('parquet').save(output_path,mode='overwrite')


if __name__ == "__main__":
        main()
