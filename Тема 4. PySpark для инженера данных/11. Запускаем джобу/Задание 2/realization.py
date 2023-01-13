spark-submit --master yarn --deploy-mode cluster partition.py 2022-05-31 /user/master/data/events /user/andrew_0/data/events


# ресурс менеджер — YARN;
# тип развёртывания — на кластере;
# 2022-05-31 — дата, данные за которую будем перекладывать;
# /user/master/data/events — первоначальная директория, слой сырых данных;
# /user/username/data/events — итоговая директория, слой ODS.