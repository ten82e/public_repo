from io import StringIO, BytesIO, TextIOWrapper
from zipfile import ZipFile
import urllib
import csv

import pyspark
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
from pyspark import SparkContext
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, Row

def read_remote_file_as_list(url,file):
    resp = urllib.request.urlopen(url)
    zipfile = ZipFile(BytesIO(resp.read()))
    data = TextIOWrapper(zipfile.open(file), encoding='utf-8')
    csv_data1 =csv.reader(data)
    list_data = [ e for e in csv_data1]
    return list_data

#train.csv(7.54 GB)
url1 = "https://storage.googleapis.com/kaggle-competitions-data/kaggle-v2/8540/862041/compressed/train.csv.zip?GoogleAccessId=web-data@kaggle-161607.iam.gserviceaccount.com&Expires=1654869506&Signature=OG4kUSOaB9Z2kEYagxPpOnLWloLtHXO1UWFVAV6OuoZB1%2F3x6o3xzOiNaoL7dFvX3Zyy%2Bsf5BZuXlJG2ETfrdbuSgwhBdTDzcrAMLbdda%2BSmYNHoHa7Sf2Qh9wdE33LvLgdCn86aqQMPyIyCeB7i8t1LYibwk0u4TgrGf6GMfNvTr2aWKAbRSROqvTuxo%2Fhjku8Z1EC6SqAfB6FFb2jDOgOv0I2Tma84ln2xdbhAK0G6cKAT0HTuLRehG28Csr39Vq%2FVuQEZe3RD1eqjYe3rtwVYizqg48wKBI2VV3tnFZSglloaY0uolbCKO5Vfmfqw1NeZTSP%2BNEcPbMI7K%2BX5Rw%3D%3D&response-content-disposition=attachment%3B+filename%3Dtrain.csv.zip"
file1 = "train.csv"
#test_supplement.csv(2.67 GB)
url2 = "https://storage.googleapis.com/kaggle-competitions-data/kaggle-v2/8540/862041/compressed/test_supplement.csv.zip?GoogleAccessId=web-data@kaggle-161607.iam.gserviceaccount.com&Expires=1654870746&Signature=c0HBqwvp28OBG1HQy3rHN2TVJRIQ28Owl8BR8kR9ekl%2FUZeBg%2BM%2FjLs5ozXFdyUcNfXEItcD%2FqHg1rJc0vEG0Nt9gqBr6qw6d%2Bs6O%2FSmroCitqvF57j%2FTWiRSfzxvrhNotS8q%2B7tOZK0lAqVxlMKEmZtFFYNGXr0xcjSNtl%2F3nXqat9F3pqTws%2BoU6UoK0NUQp03p12ajDYRuDqf52FkTzGqe6DW%2B5Ue6ETPS6E1sgN2PKuONUDsJijb%2FzhCwm7qp3xZ9mCZPUwWBpvnGp4VB%2BgL7GS6jxRPlJ3MQQ5e7%2FD%2B6WX%2Fg1jiiOVnaNkCQtSHhJwgH86O7QnVFoMM4BNd8w%3D%3D&response-content-disposition=attachment%3B+filename%3Dtest_supplement.csv.zip"
file2 = "test_supplement.csv"
#test.csv(863.27 MB)
url3 = "https://storage.googleapis.com/kaggle-competitions-data/kaggle-v2/8540/862041/compressed/test.csv.zip?GoogleAccessId=web-data@kaggle-161607.iam.gserviceaccount.com&Expires=1654870818&Signature=eZdk0SR4lcNZPCvq8tPBRQ%2BCifN2V0Pkgmp6x85esly7n%2Bp9FyfQpqvVqJNwZhOMuVs6%2FMSE9HYZXArS8N8y1se%2FIU0tel%2BpYOU0xZs%2Fci07Ns8%2FCrw8Q6xxXVJ6uOH0Ql84TQ51CHkGZ8tjW%2FR12kRXUDBKvWyMTsCTcnibOotzOvz3FP4BKyvR%2F0aIz2b0y1tN%2FCO5xTpio4XuyFOIyH3Y5Bh%2BiRDenwjBj4BO5jYVtiBJVuAZl4wklymDTMHrl9CahHhma2avQAAgVTuW1eTjXO44RkkbsnSNO6pNATEoJB0mEkZBy7HV05UU0M6Yjb%2BIFwRFc7vpsAxTu%2FGUMQ%3D%3D&response-content-disposition=attachment%3B+filename%3Dtest.csv.zip"
file3 = "test.csv"
#train_sample.csv(4.08 MB)
url4 = "https://storage.googleapis.com/kaggle-competitions-data/kaggle-v2/8540/862041/compressed/train_sample.csv.zip?GoogleAccessId=web-data@kaggle-161607.iam.gserviceaccount.com&Expires=1654876534&Signature=q1utBLMay27j10NyMWTXnz0%2F%2F%2BUiUvrD8WAEZVaTt399AM9cT51DmYcvnLCMJxjhVj3UikaGhopfnqIkmxZDiQnspFZTHJNSMKAaB87SDGxEfbxu5DQ9Kv89jl7OaSVtaa24FTv29dDMgt7qbjd2%2BZG7yeC27hOPmOo47DktUnX%2BY4iQFkJUukPOLeAuhhf6hjXeo%2Fs0HMuBYXIghFsxLYFsu3o6RERs4h%2B3GdTMOYxMVD0oNiXobM6YqqMPA29bGNqa7123njKYsT%2FcFXCBorrmIBejOiwPmMqsaEMHlaHXdjXPv5mr1s4CTEwt6xhtNgs%2F5VOKBk%2Fgl7ft19nZ7Q%3D%3D&response-content-disposition=attachment%3B+filename%3Dtrain_sample.csv.zip"
file4 = "train_sample.csv"

spark_context = SparkContext()
spark = SparkSession(spark_context)


list1 = read_remote_file_as_list(url1,file1)
df1 = spark.createDataFrame(data=list1[1:], schema = list1[0])

list2 = read_remote_file_as_list(url2,file2)
df2 = spark.createDataFrame(data=list2[1:], schema = list2[0])

list3 = read_remote_file_as_list(url3,file3)
df3 = spark.createDataFrame(data=list3[1:], schema = list3[0])

df1n = df1.drop('attributed_time') \
        .drop("is_attributed")

df2n = df2.drop('click_id')
df3n = df3.drop('click_id')

df4 = df1n.unionAll(df2n)
df5 = df4.unionAll(df3n) \
        .count()