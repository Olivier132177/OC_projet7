from PIL import Image
import numpy as np
from keras.applications.vgg16 import VGG16,preprocess_input
from keras import models
from keras.preprocessing.image import img_to_array
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
def prediction(partition):
    model = VGG16()
    model = models.Model(inputs=model.inputs, outputs=model.layers[-2].output)
    for parti in partition :
        feat=model.predict(parti[1])
        yield (parti[0],feat)
def pretraitement(x):
    imag=Image.frombytes('RGB',((x[2],x[1])),bytes(x[3]))
    imag=img_to_array(np.array(imag.resize((224,224))))
    imag=preprocess_input(imag.reshape((1, imag.shape[0], imag.shape[1], imag.shape[2])))
    return (x[0],imag)
path='s3://oliviergollnick/Images_Train/*/*.jpg'
sc=SparkContext()
sqlContext=SQLContext(sc)
df=sqlContext.read.format("image").load(path)
rdd=df.select('image.origin','image.height','image.width','image.data').rdd
rdd_etape1=rdd.map(lambda x : pretraitement(x))
rdd_final=rdd_etape1.mapPartitions(prediction)
rdd_final2=rdd_final.map(lambda x : (x[0],(x[1][0]).tolist()))
df_fin=sqlContext.createDataFrame(rdd_final2,['adresse','image_transformee'])
df_fin.write.parquet('s3://oliviergollnick/resultats',mode='overwrite')
#pour lire ensuite : df_images = spark.read.parquet('s3://oliviergollnick/resultats')