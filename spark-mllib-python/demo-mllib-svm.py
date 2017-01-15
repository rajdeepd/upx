
# coding: utf-8

# In[1]:

import os
import sys
from constants import SPARK_HOME
os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.1-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[2]:

#from pyspark import SparkContext
#sc = SparkContext() 


# In[3]:

# Load the data
data = sc.textFile("../spark-mllib-datasets/datasets-mllib-demo/dataset-mllib-svm.txt")


# In[4]:

from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint

# parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])


# In[5]:

# transform the raw dataset into a RDD
parsedData = data.map(parsePoint)

parsedData.take(5)


# In[6]:

from time import time

# Build the model
t0 = time()
model_svm = SVMWithSGD.train(parsedData, iterations=3)
tt = time() - t0

print "Classifier trained in {} seconds".format(round(tt,3))


# In[7]:

# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model_svm.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))

