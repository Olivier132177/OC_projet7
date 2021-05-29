import pandas as pd
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
sc=SparkContext()
sqlContext=SQLContext(sc)

df_images = spark.read.parquet('s3://oliviergollnick/resultats')

dfp=df_images.take(50)
df_pand=pd.DataFrame(dfp)
list_df=df_pand[1].to_list()
df_matrice=pd.DataFrame(list_df)
matr_para=sc.parallelize(df_matrice)
df_matrice.index=df_pand[0]
df_final=sqlContext.createDataFrame(df_matrice)
df_final.write.csv('s3://oliviergollnick/df_csv')

df_matrice=df_images.select(df_images.image_transformee.cast('string'))
df_matrice.write.csv('s3://oliviergollnick/resultats_csv')


rdd_im=df_images.select('image_transformee').rdd
rdd_im2=rdd_im.map(lambda x : x[0])
rdd_im2.saveAsTextFile('s3://oliviergollnick/images_reduites_csv')

df_m=rdd_m.to_DF()
df_m.write.csv('s3://oliviergollnick/test_df_1922')
rdd_m.saveAsTextFile('s3://oliviergollnick/test1908')

bbb.write.csv('s3://oliviergollnick/test_df_1922')
