
# coding: utf-8

# In[1]:

import os
import sys
from constants import SPARK_HOME
os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.1-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[3]:

# Load the data
data = sc.textFile("../spark-mllib-datasets/datasets-mllib-demo/dataset-mllib-kmeans.txt")


# In[4]:

from numpy import array
from math import sqrt

from pyspark.mllib.clustering import KMeans, KMeansModel

# parse the data
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))
parsedData.take(5)


# In[6]:

from time import time

# Build the model (cluster the data)
t0 = time()
clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")
tt = time() - t0

print "Classifier trained in {} seconds".format(round(tt,3))


# In[7]:

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

