
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

# Load the data
data = sc.textFile("../spark-mllib-datasets/datasets-mllib-demo/dataset-mllib-regression.data")


# In[3]:

# Load libraries
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel

# parse the data
def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])


# In[4]:

# transform the raw dataset into a RDD of LabelelPoint
parsedData = data.map(parsePoint)


# In[5]:

from time import time

# Build the model 
t0 = time()
model_linear_sgd = LinearRegressionWithSGD.train(parsedData, iterations=3, step=0.1) 
tt = time() - t0

print "Classifier trained in {} seconds".format(round(tt,3))


# In[6]:

# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.label, model_linear_sgd.predict(p.features)))


# In[7]:

# Calculate error
MSE = valuesAndPreds     .map(lambda (v, p): (v - p)**2)     .reduce(lambda x, y: x + y) / valuesAndPreds.count()
print "Mean Squared Error = " + str(MSE)


# In[ ]:



