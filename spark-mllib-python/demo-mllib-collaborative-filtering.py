
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
data = sc.textFile("../spark-mllib-datasets/datasets-mllib-demo/dataset-mllib-als.data")


# In[3]:

# Load libraries
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

# Parse the data
ratings = data.map(lambda l: l.split(','))    .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
    
ratings.take(5)


# In[4]:

from time import time

# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 3

t0 = time()
model = ALS.train(ratings, rank, numIterations)
tt = time() - t0

print "Classifier trained in {} seconds".format(round(tt,3))


# In[5]:

# Evaluate the model on training data
testdata = ratings.map(lambda p: (p[0], p[1]))


# In[6]:

predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))


# In[7]:

ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)


# In[8]:

MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))


# In[ ]:



