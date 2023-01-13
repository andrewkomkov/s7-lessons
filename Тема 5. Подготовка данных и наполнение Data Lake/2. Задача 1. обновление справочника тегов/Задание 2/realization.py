import datetime
import pyspark.sql.functions as F 
import dateutil.relativedelta

def input_paths(date,depth):
    range_ = []
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    i = 0
    while i < depth:
        date_2 = date - dateutil.relativedelta.relativedelta(days=i)
        path_ = f'/user/andrew_0/data/events/date={date_2.strftime("%Y-%m-%d")}/event_type=message'
        range_.append(path_)
        i = i+1
    return(range_)


range_ = input_paths(date_from,depth)

df = spark.read.parquet(*range_)

df_explode = df.withColumn("tag", F.explode(df.event.tags)).where("event.message_channel_to is not null")
df_explode = df_explode.select(df_explode.event.message_from.alias("message_from"),df_explode.tag)

df_explode = df_explode.groupBy('tag') \
                      .agg(F.countDistinct('message_from').alias("suggested_count")) \
                      .filter(F.col('suggested_count') >= 100) \
                      .orderBy(F.col("suggested_count").desc()) \
                      .join(tags_verified, ['tag'], how='left_anti')

df_explode.write.format('parquet').save('/user/andrew_0/data/analytics/candidates_d7_pyspark',mode='overwrite')