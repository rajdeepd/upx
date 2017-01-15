
# coding: utf-8

# In[4]:

import os
import sys
from constants import SPARK_HOME
os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.1-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# Note : Please download kddcup.data.gz from the following url http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz and place it under folder `datasets-mllib-datasets`.
# Also copy http://kdd.ics.uci.edu/databases/kddcup99/corrected.gz which will be used later.

# In[11]:

import urllib
#f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz", "kddcup.data.gz")

data_file = "../spark-mllib-datasets/kddcup.data.gz"

raw_data = sc.textFile(data_file)

print "Train data size is {}".format(raw_data.count())


# In[13]:

# load test data
test_data_file = "../spark-mllib-datasets//corrected.gz"
test_data_raw = sc.textFile(test_data_file)

print "Test data size is {}".format(test_data_raw.count())


# In[14]:

from pyspark.mllib.regression import LabeledPoint
from numpy import array

def parse_interaction(line):
    line_split = line.split(",")
    # remove 1,2,3,41
    clean_line_split = line_split[0:1]+line_split[4:41]
    attack = 1.0
    if line_split[41] == 'normal.':
        attack = 0.0
    return LabeledPoint(attack, array([float(x) for x in clean_line_split]))


training_data = raw_data.map(parse_interaction)
training_data.take(5)


# In[15]:

# prepare test data
test_data = test_data_raw.map(parse_interaction)


# In[16]:

# train classifier
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from time import time

# Build logistic model
t0 = time()
logit_model = LogisticRegressionWithLBFGS.train(training_data)
tt = time() - t0

print "Classifier trained in {} seconds".format(round(tt,3))


# In[9]:

# Evaluating on new data
labels_preds = test_data.map(lambda p: (p.label, logit_model.predict(p.features)))


# In[10]:

# calculate the classification error
t0 = time() 
test_accuracy = labels_preds.filter(lambda (v, p): v == p).count() / float(test_data.count()) 
tt = time() - t0 

print "Prediction made in {} seconds. Test accuracy is {}".format(round(tt,3), round(test_accuracy,4))


# In[9]:



