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