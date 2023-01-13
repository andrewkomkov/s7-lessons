import sys
from datetime import date as dt


def main():
#     client_name = sys.argv[1]
    date = sys.argv[1]
#     dir_name = sys.argv[3]
    
#     if str(dt.today()) != date:
# #         print(f"Hi {client_name}!")
# #         current_dir = dir_name
# #         print(f'Your current directory is {current_dir}')
#     else:
#         print("Come back another day")
#         return

    import findspark
    findspark.init()
    findspark.find()
    import os
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    import pyspark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName("akk_2") \
                        .config("spark.executor.memory", "5530m")\
                        .config("spark.executor.instances", 4)\
                        .config("spark.executor.cores", 2)\
                        .config("spark.files.overwrite",True)
                        .getOrCreate()
    
    events_for_date = spark.read.json(f"/user/master/data/events/date={date}")
    events_for_date.write.partitionBy("event_type")\
                    .parquet(f'/user/andrew0/data/{date}/events_p/')
    
    print(f'All events per {date} loaded to ODS')

# Добавляем переменные в код

# Если дата, передаваемая через командную строку, совпадёт с актуальной датой,
# то программа поздоровается с пользователем и передаст название текущей директории.
# В противном случае попросит прийти в другой день.

        
# При запуске файла будет выполняться код ниже после двоеточия
# Конкретно здесь - функция main()

if __name__ == "__main__":
        main()
