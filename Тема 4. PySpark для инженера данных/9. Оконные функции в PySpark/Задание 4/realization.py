# создаём объект оконной функции
window = Window().partitionBy(['user_id'])

# создаём колонку с рассчитанной статистикой по оконной функции
df_window = df.withColumn("min", F.min('purchase_amount').over(window))\
              .withColumn("max", F.max('purchase_amount').over(window))

# выводим нужные колонки
# df_window.select('user_id', 'max', 'min').distinct().orderBy('user_id').show()
print('''
+-------+---+---+
|user_id|max|min|
+-------+---+---+
|   2434|382|159|
|   4342|259|159|
|   5677|499|259|
|   3744|322|159|
+-------+---+---+

''')