def tag_tops(date_from,depth,spark):
    
    range_ = input_paths(date_from,depth)
    
    df = spark.read.parquet(*range_)
    
    window_  = Window.partitionBy(["user"]).orderBy(F.col("suggested_count").desc(),F.col("tag").desc())
    
    
    df = df.where("event.message_channel_to is not null")\
    .select([F.col("event.message_from").alias("user"), F.explode(F.col("event.tags")).alias("tag")])\
    .groupBy(["user","tag"]).agg(F.count("tag").alias("suggested_count"))\
    .withColumn("r_n",F.row_number().over(window_))\
    .withColumn("tag_top_1",F.lead("tag",0).over(window_))\
    .withColumn("tag_top_2",F.lead("tag",1).over(window_))\
    .withColumn("tag_top_3",F.lead("tag",2).over(window_))\
    .where("r_n = 1")\
    .select([F.col('user').alias('user_id'),F.col('tag_top_1'),F.col('tag_top_2'),F.col('tag_top_3')])
    
    print(date_from,depth)
    return df